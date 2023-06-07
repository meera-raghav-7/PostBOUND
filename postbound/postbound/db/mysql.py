"""Contains the MySQL implementation of the Database interface."""
from __future__ import annotations
import functools
import configparser
import dataclasses
import json
import os
import textwrap
from collections.abc import Sequence,Iterable
import sys
from dataclasses import dataclass

from typing import Any, Optional,Union

import mysql.connector

from postbound.db import db
from postbound.qal import qal, base, clauses, transform, formatter,expressions
from postbound.optimizer import jointree
from postbound.optimizer.physops import operators as physops
from postbound.optimizer.planmeta import hints as planmeta
from postbound.util import collections as collection_utils, logging, misc , typing as type_utils



@dataclasses.dataclass(frozen=True)
class MysqlConnectionArguments:
    user: str
    database: str
    password: str = ""
    host: str = "127.0.0.1"
    port: int = 3306
    use_unicode: bool = True
    charset: str = "utf8mb4"
    autocommit: bool = True
    sql_mode: str = "ANSI"

    def parameters(self) -> dict[str, str | int | bool]:
        return dataclasses.asdict(self)


class MysqlInterface(db.Database):
    def __init__(self, connection_args: MysqlConnectionArguments, system_name: str = "MySQL", *,
                 cache_enabled: bool = True) -> None:
        self.connection_args = connection_args
        self._cnx = mysql.connector.connect(**connection_args.parameters())
        self._cur = self._cnx.cursor(buffered=True)

        self._db_schema = MysqlSchemaInterface(self)
        self._db_stats = MysqlStatisticsInterface(self)
        super().__init__(system_name, cache_enabled=cache_enabled)

    def schema(self) -> db.DatabaseSchema:
        return self._db_schema
    
    def hinting(self) -> db.HintService:
        return MysqlHintService()

    
    def optimizer(self) -> db.OptimizerInterface:
        return MysqlOptimizer(self)

    def statistics(self, emulated: bool | None = None, cache_enabled: Optional[bool] = None) -> db.DatabaseStatistics:
        if emulated is not None:
            self._db_stats.emulated = emulated
        if cache_enabled is not None:
            self._db_stats.cache_enabled = cache_enabled
        return self._db_stats

    def execute_query(self, query: qal.SqlQuery | str, *, cache_enabled: Optional[bool] = None) -> Any:
        cache_enabled = cache_enabled or (cache_enabled is None and self._cache_enabled)
        query = self._prepare_query_execution(query)

        if cache_enabled and query in self._query_cache:
            query_result = self._query_cache[query]
        else:
            self._cur.execute(query)
            query_result = self._cur.fetchall()
            if cache_enabled:
                self._query_cache[query] = query_result

        # simplify the query result as much as possible: [(42, 24)] becomes (42, 24) and [(1,), (2,)] becomes [1, 2]
        # [(42, 24), (4.2, 2.4)] is left as-is
        if not query_result:
            return []
        result_structure = query_result[0]  # what do the result tuples look like?
        if len(result_structure) == 1:  # do we have just one column?
            query_result = [row[0] for row in query_result]  # if it is just one column, unwrap it
        return query_result if len(query_result) > 1 else query_result[0]  # if it is just one row, unwrap it

    def cardinality_estimate(self, query: qal.SqlQuery | str) -> int:
        query = self._prepare_query_execution(query, drop_explain=True)
        query_plan = self._obtain_query_plan(query)["query_block"]

        if "nested_loop" in query_plan:
            final_join = query_plan["nested_loop"][-1]
        else:
            final_join = query_plan

        return final_join["table"]["rows_produced_per_join"]

    def cost_estimate(self, query: qal.SqlQuery | str) -> float:
        query = self._prepare_query_execution(query, drop_explain=True)
        query_plan = self._obtain_query_plan(query)
        return query_plan["query_block"]["cost_info"]["query_cost"]

    def database_name(self) -> str:
        self._cur.execute("SELECT DATABASE();")
        db_name = self._cur.fetchone()[0]
        return db_name

    def database_system_version(self) -> misc.Version:
        self._cur.execute("SELECT VERSION();")
        version = self._cur.fetchone()[0]
        return misc.Version(version)

    def describe(self) -> dict:
        base_info = {
            "system_name": self.database_system_name(),
            "system_version": self.database_system_version(),
            "database": self.database_name(),
            "statistics_settings": {
                "emulated": self._db_stats.emulated,
                "cache_enabled": self._db_stats.cache_enabled
            }
        }
        self._cur.execute("SHOW VARIABLES")
        system_config = self._cur.fetchall()
        base_info["system_settings"] = dict(system_config)
        return base_info

    def reset_connection(self) -> None:
        self._cur.close()
        self._cnx.cmd_reset_connection()
        self._cur = self._cnx.cursor()

    def cursor(self) -> db.Cursor:
        return self._cur

    def close(self) -> None:
        self._cur.close()
        self._cnx.close()

    def _prepare_query_execution(self, query: qal.SqlQuery | str, *, drop_explain: bool = False) -> str:
        """Provides the query in a unified format, taking care of preparatory statements as necessary.

        `drop_explain` can be used to remove any EXPLAIN clauses from the query. Note that all actions that require
        the "semantics" of the query to be known (e.g. EXPLAIN modifications or query hints) and are therefore only
        executed for instances of the qal queries.
        """
        if not isinstance(query, qal.SqlQuery):
            return query

        if drop_explain:
            query = transform.drop_clause(query, clauses.Explain)
        if query.hints and query.hints.preparatory_statements:
            self._cur.execute(query.hints.preparatory_statements)
            query = transform.drop_hints(query, preparatory_statements_only=True)
        return str(query)

    def _obtain_query_plan(self, query: str) -> dict:
        if not query.startswith("EXPLAIN FORMAT = JSON"):
            query = "EXPLAIN FORMAT = JSON " + query
        self._cur.execute(query)
        result = self._cur.fetchone()[0]
        return json.loads(result)


class MysqlSchemaInterface(db.DatabaseSchema):
    def __init__(self, mysql_db: MysqlInterface):
        super().__init__(mysql_db)

    def lookup_column(self, column: base.ColumnReference | str,
                      candidate_tables: list[base.TableReference]) -> base.TableReference:
        column = column.name if isinstance(column, base.ColumnReference) else column
        for table in candidate_tables:
            table_columns = self._fetch_columns(table)
            if column in table_columns:
                return table
        candidate_tables = [tab.full_name for tab in candidate_tables]
        raise ValueError(f"Column {column} not found in candidate tables {candidate_tables}")

    def is_primary_key(self, column: base.ColumnReference) -> bool:
        if not column.table:
            raise base.UnboundColumnError(column)
        index_map = self._fetch_indexes(column.table)
        return index_map.get(column.name, False)

    def has_secondary_index(self, column: base.ColumnReference) -> bool:
        if not column.table:
            raise base.UnboundColumnError(column)
        index_map = self._fetch_indexes(column.table)

        # The index map contains an entry for each attribute that actually has an index. The value is True, if the
        # attribute (which is known to be indexed), is even the Primary Key
        # Our method should return False in two cases: 1) the attribute is not indexed at all; and 2) the attribute
        # actually is the Primary key. Therefore, by assuming it is the PK in case of absence, we get the correct
        # value.
        return not index_map.get(column.name, True)

    def datatype(self, column: base.ColumnReference) -> str:
        if not column.table:
            raise base.UnboundColumnError(column)
        query_template = "SELECT column_type FROM information_schema.columns WHERE table_name = %s AND column_name = %s"
        self._db.cursor().execute(query_template, (column.table.full_name, column.name))
        result_set = self._db.cursor().fetchone()
        return str(result_set[0])

    def _fetch_columns(self, table: base.TableReference) -> list[str]:
        query_template = "SELECT column_name FROM information_schema.columns WHERE table_name = %s"
        self._db.cursor().execute(query_template, (table.full_name,))
        result_set = self._db.cursor().fetchall()
        return [col[0] for col in result_set]

    def _fetch_indexes(self, table: base.TableReference) -> dict[str, bool]:
        index_query = textwrap.dedent("""
            SELECT column_name, column_key = 'PRI'
            FROM information_schema.columns
            WHERE table_name = %s AND column_key <> ''
        """)
        self._db.cursor().execute(index_query, table.full_name)
        result_set = self._db.cursor().fetchall()
        index_map = dict(result_set)
        return index_map


class MysqlStatisticsInterface(db.DatabaseStatistics):
    def __init__(self, mysql_db: MysqlInterface):
        super().__init__(mysql_db)

    def _retrieve_total_rows_from_stats(self, table: base.TableReference) -> Optional[int]:
        count_query = "SELECT table_rows FROM information_schema.tables WHERE table_name = %s"
        self._db.cursor().execute(count_query, table.full_name)
        count = self._db.cursor().fetchone()[0]
        return count

    def _retrieve_distinct_values_from_stats(self, column: base.ColumnReference) -> Optional[int]:
        stats_query = "SELECT cardinality FROM information_schema.statistics WHERE table_name = %s AND column_name = %s"
        self._db.cursor().execute(stats_query, (column.table.full_name, column.name))
        distinct_vals: Optional[int] = self._db.cursor().fetchone()
        if distinct_vals is None and not self.enable_emulation_fallback:
            return distinct_vals
        elif distinct_vals is None:
            return self._calculate_distinct_values(column, cache_enabled=True)
        else:
            return distinct_vals

    def _retrieve_min_max_values_from_stats(self, column: base.ColumnReference) -> Optional[tuple[Any, Any]]:
        if not self.enable_emulation_fallback:
            raise db.UnsupportedDatabaseFeatureError(self._db, "min/max value statistics")
        return self._calculate_min_max_values(column, cache_enabled=True)

    def _retrieve_most_common_values_from_stats(self, column: base.ColumnReference,
                                                k: int) -> Sequence[tuple[Any, int]]:
        if not self.enable_emulation_fallback:
            raise db.UnsupportedDatabaseFeatureError(self._db, "most common values statistics")
        return self._calculate_most_common_values(column, k=k, cache_enabled=True)


MysqlOptimizerSettings = {
    physops.JoinOperators.IndexMergeJoin: "index_merge",
    physops.JoinOperators.BlockNestedLoopJoin: "block_nested_loop",
    physops.JoinOperators.HashJoin: "hash_join"
}


MysqlOptimizerHints = {
    physops.JoinOperators.BlockNestedLoopJoin: "BNL",
    physops.JoinOperators.IndexMergeJoin: "INDEX_MERGE",
    physops.JoinOperators.IndexJoin: "INDEX",
    physops.JoinOperators.MergeJoin: "MERGE",
    physops.ScanOperators.IndexScan: "INDEX_SCAN",
    physops.JoinOperators.HashJoin: "HASH_JOIN"
}



    
MysqlJoinHints = {physops.JoinOperators.BlockNestedLoopJoin, physops.JoinOperators.IndexMergeJoin, 
                  physops.JoinOperators.MergeJoin, physops.JoinOperators.IndexJoin, physops.JoinOperators.HashJoin}

MysqlScanHints = {physops.ScanOperators.IndexScan}

MysqlPlanHints = {planmeta.HintType.JoinOrderHint}

@dataclass
class HintParts:
    """Captures the different kinds of Postgres-hints to collect them more easily."""
    settings: list[str]
    hints: list[str]

    @staticmethod
    def empty() -> HintParts:
        """An empty hint parts object, i.e. no hints have been specified, yet."""
        return HintParts([], [])

    def merge_with(self, other: HintParts) -> HintParts:
        """Combines the hints that are contained in this hint parts object with all hints in the other object.

        This construct new hint parts and leaves the current object unmodified.
        """
        merged_settings = self.settings + [setting for setting in other.settings if setting not in self.settings]
        merged_hints = self.hints + [hint for hint in other.hints if hint not in self.hints]
        return HintParts(merged_settings, merged_hints)

def _generate_join_key(tables: Iterable[base.TableReference]) -> str:
    """Builds a MySQL-compatible identifier for the join consisting of the given tables."""
    return ", ".join(tab.identifier() for tab in tables)
  


def _generate_mysql_operator_hints(physical_operators: physops.PhysicalOperatorAssignment) -> HintParts:
    """Generates the hints and preparatory statements to enforce the selected optimization ."""
    settings = []
    for operator, enabled in physical_operators.global_settings.items():
        if enabled is None:
            setting = "default"
        else:
            setting = "on" if enabled else "off"
        operator_key = MysqlOptimizerSettings[operator]
        settings.append(f"SET optimizer_switch='{operator_key}={setting}';")

    hints = []
    for table, scan_assignment in physical_operators.scan_operators.items():
        table_key = table.identifier()
        scan_assignment = MysqlOptimizerHints[scan_assignment.operator]
        hints.append(f"{scan_assignment}({table_key})")

    if hints:
        hints.append("")
    for join, join_assignment in physical_operators.join_operators.items():
        join_key = _generate_join_key(join)
        join_assignment = MysqlOptimizerHints[join_assignment.operator]
        hints.append(f"{join_assignment}({join_key})")

    if not settings and not hints:
        return HintParts.empty()

    return HintParts(settings, hints)


def _generate_mysql_index_hints(plan_parameters: planmeta.PlanParameterization) -> HintParts:
    hints, settings = [], []

    
    for table, index_list in plan_parameters.index_hints.items():
        for index in index_list:
            if table:
                index = ", ".join(index_list)
                hints.append(f"FORCE_INDEX({table.identifier()} {index})")
                break  
            else:
                hints.append(f"FORCE_INDEX({', '.join(index_list)})")
                break
    
    for tables in plan_parameters.join_order_hints.keys():
        hint = ', '.join(table.identifier() for table in tables)
        hints.append(f"JOIN_ORDER({hint})")
    


    return HintParts(settings, hints)    

    

def _generate_hint_block(parts: HintParts) -> Optional[clauses.Hint]:
    """Constructs the hint block for the given hint parts"""
    settings, hints = parts.settings, parts.hints
    if not settings and not hints:
        return None
    settings_block = "\n".join(settings)
    hints_block = "\n".join(["/*+"] + ["  " + hint for hint in hints] + ["*/"]) if hints else ""
    return clauses.Hint(settings_block, hints_block)


def _apply_hint_block_to_query(query: qal.SqlQuery, hint_block: Optional[clauses.Hint]) -> qal.SqlQuery:
    """Generates a new query with the given hint block."""
    return transform.add_clause(query, hint_block) if hint_block else query

def modify_str_methods():
    string_method = expressions.StaticValueExpression.__str__
    cast_exp = expressions.CastExpression.__str__
    exp_method = clauses.Explain.__str__
    def typecasting(cast_expression):
        if type(cast_expression) == expressions.CastExpression:
            sys.exit("MySQL doesn't support type casting.")

    def double_quotes(expression):
        if type(expression) == expressions.StaticValueExpression and type(expression.value) == str:
            return f'"{expression.value}"'
        return string_method(expression)
    
    def modified_explain_str(exp_method) -> str:
            explain_prefix = "EXPLAIN"
            explain_body = ""
            if exp_method.analyze and exp_method.target_format:
                explain_body = f" FORMAT = {exp_method.target_format}"
            elif exp_method.analyze:
                explain_body = " ANALYZE"
            elif exp_method.target_format:
                explain_body = f" FORMAT = {exp_method.target_format}"
            return explain_prefix + explain_body
    
    expressions.CastExpression.__str__ = typecasting
    expressions.StaticValueExpression.__str__ = double_quotes
    clauses.Explain.__str__ = modified_explain_str
    
    return string_method, cast_exp,exp_method

class MysqlHintService(db.HintService):


    def generate_hints(self, query: qal.SqlQuery,
                       join_order: Optional[jointree.LogicalJoinTree | jointree.PhysicalQueryPlan] = None,
                       physical_operators: Optional[physops.PhysicalOperatorAssignment] = None,
                       plan_parameters: Optional[planmeta.PlanParameterization] = None) -> qal.SqlQuery:
        
        join_order
        hint_parts = None
        hint_parts = hint_parts if hint_parts else HintParts.empty()
        if physical_operators:
            operator_hints = _generate_mysql_operator_hints(physical_operators)
            hint_parts = hint_parts.merge_with(operator_hints)
        
        if plan_parameters:
            plan_hints = _generate_mysql_index_hints(plan_parameters)
            hint_parts = hint_parts.merge_with(plan_hints)


        hint_block = _generate_hint_block(hint_parts)
        query = _apply_hint_block_to_query(query, hint_block)
        return query

    def format_query(self, query: qal.SqlQuery) -> str:
        
        string_method, cast_exp,exp_method  = modify_str_methods()
        query = formatter.format_quick(query)
        expressions.StaticValueExpression.__str__ = string_method
        expressions.CastExpression.__str__ = cast_exp
        clauses.Explain.__str__ = exp_method
        return query
        
    
    def supports_hint(self, hint: physops.PhysicalOperator | planmeta.HintType) -> bool:
        """Checks, whether the database system is capable of using the specified hint or operator."""
        return hint in MysqlJoinHints | MysqlScanHints | MysqlPlanHints
    
class MysqlOptimizer(db.OptimizerInterface):
    def __init__(self, mysql_instance: MysqlInterface) -> None:
        self._mysql_instance = mysql_instance

    def query_plan(self, query: qal.SqlQuery | str) -> db.QueryExecutionPlan:
        return None

    def cardinality_estimate(self, query: qal.SqlQuery | str) -> int:
        return None

    def cost_estimate(self, query: qal.SqlQuery | str) -> float:
        return None

def _parse_mysql_connection(config_file: str) -> MysqlConnectionArguments:
    config = configparser.ConfigParser()
    config.read(config_file)
    if not "MYSQL" in config:
        raise ValueError("Malformed MySQL config file: no [MYSQL] section found.")
    mysql_config = config["MYSQL"]

    if "User" not in mysql_config or "Database" not in mysql_config:
        raise ValueError("Malformed MySQL config file: 'User' and 'Database' keys are required in the [MYSQL] section.")
    user = mysql_config["User"]
    database = mysql_config["Database"]

    optional_settings = {}
    for key in ["Password", "Host", "Port", "UseUnicode", "Charset", "AutoCommit", "SqlMode"]:
        if key not in mysql_config:
            continue
        optional_settings[misc.camel_case2snake_case(key)] = mysql_config[key]
    return MysqlConnectionArguments(user, database, **optional_settings)


def connect(*, name: str = "mysql", connection_args: Optional[MysqlConnectionArguments] = None,
            config_file: str = ".mysql_connection.config",
            cache_enabled: Optional[bool] = None, private: bool = False) -> MysqlInterface:
    db_pool = db.DatabasePool.get_instance()
    if config_file and not connection_args:
        if not os.path.exists(config_file):
            raise ValueError("Config file was given, but does not exist: " + config_file)
        connection_args = _parse_mysql_connection(config_file)
    elif not connection_args:
        raise ValueError("Connect string or config file are required to connect to Postgres")

    mysql_db = MysqlInterface(connection_args, system_name=name, cache_enabled=cache_enabled)
    if not private:
        db_pool.register_database(name, mysql_db)
    return mysql_db

from __future__ import annotations
import json
import collections
import concurrent
import concurrent.futures
import os
import textwrap
import threading
import re
from typing import Any


import mysql.connector


from postbound.db import db
from postbound.qal import qal, base, transform
from postbound.util import logging, misc as utils


class mysqlInterface(db.Database):
    
    def __init__(self, connect_string: dict[str, Any],system_name: str = "MYSQL",  *, cache_enabled: bool = True) -> None:
       
        self.connect_string = connect_string
        
        self._connection = mysql.connector.connect(**connect_string)
        self._connection.autocommit = True
        self._cursor = self._connection.cursor()
       
        self._db_schema = mysqlSchemaInterface(self)
        self._db_stats = mysqlStatisticsInterface(self)

        super().__init__(system_name, cache_enabled=cache_enabled)

    def schema(self) -> db.DatabaseSchema:
        return self._db_schema
    
    def statistics(self, emulated: bool | None = None, cache_enabled: bool | None = None) -> db.DatabaseStatistics:
        if emulated is not None:
            self._db_stats.emulated = emulated
        if cache_enabled is not None:
            self._db_stats.cache_enabled = cache_enabled
        return self._db_stats
        

    def execute_query(self, query: qal.SqlQuery | str, *, cache_enabled: bool | None = None) -> Any:
        cache_enabled = cache_enabled or (cache_enabled is None and self._cache_enabled)

        if isinstance(query, qal.SqlQuery):
            if query.hints and query.hints.preparatory_statements:
                self._cursor.execute(query.hints.preparatory_statements)
            query = transform.drop_hints(query, preparatory_statements_only=True)
            query = str(query)

        if cache_enabled and query in self._query_cache:
            query_result = self._query_cache[query]
        else:
            self._cursor.execute(query)
            query_result = self._cursor.fetchall()
            if cache_enabled:
                self._query_cache[query] = query_result

        
        if not query_result:
            return []
        result_structure = query_result[0]  
        if len(result_structure) == 1:  
            query_result = [row[0] for row in query_result]  
        return query_result if len(query_result) > 1 else query_result[0]
    
    def cardinality_estimate(self, query: qal.SqlQuery | str) -> int:
        query = str(query)
        if not query.upper().startswith("EXPLAIN FORMAT=JSON"):
            query = "EXPLAIN FORMAT=JSON " + query
        self._cursor.execute(query)
        query_plan_json = self._cursor.fetchone()[0]
        query_plan = json.loads(query_plan_json)
        query_block = query_plan.get("query_block")
        if query_block and "table" in query_block:
            estimate = int(query_block["table"]["rows_examined_per_scan"])
        else:
            estimate = 1
        return estimate
    
    
   
    
    def database_name(self) -> None:
        self._cursor.execute("SELECT DATABASE();")
        db_name = self._cursor.fetchone()[0]
        return db_name
        
    
    def database_system_version(self) -> utils.Version:
        self._cursor.execute("SELECT VERSION();")
        mysql_ver = self._cursor.fetchone()[0]
        # version looks like "8.0.32"
        return utils.Version(str(mysql_ver))

        
            

    def reset_connection(self) -> None:
        self._cursor.close()
        self._connection.rollback()
        self._cursor = self._connection.cursor()

    def cursor(self) -> db.Cursor:
        return self._cursor

    def close(self) -> None:
        self._cursor.close()
        self._connection.close()


class mysqlSchemaInterface(db.DatabaseSchema):
    """Schema-specific parts of the general MYSQL interface."""

    def __init__(self, mysql_db: mysqlInterface) -> None:
        super().__init__(mysql_db)

    def lookup_column(self, column: base.ColumnReference,
                      candidate_tables: list[base.TableReference]) -> base.TableReference:
        for table in candidate_tables:
            table_columns = self._fetch_columns(table)
            if column.name in table_columns:
                return table
        candidate_tables = [table.full_name for table in candidate_tables]
        raise ValueError("Column '{}' not found in candidate tables {}".format(column.name, candidate_tables))
    
    def is_primary_key(self, column: base.ColumnReference) -> bool:
        if not column.table:
            raise base.UnboundColumnError(column)
        index_map = self._fetch_indexes(column.table)
        return index_map.get(column.name, False)
    
    def has_secondary_index(self, column: base.ColumnReference) -> bool:
        if not column.table:
            raise base.UnboundColumnError(column)
        index_map = self._fetch_indexes(column.table)
        return not index_map.get(column.name, True)
    
    def datatype(self, column: base.ColumnReference) -> str:
        if not column.table:
            raise base.UnboundColumnError(column)
        query_template = textwrap.dedent("""
                        SELECT data_type FROM information_schema.columns
                        WHERE BINARY table_name = '{tab}' AND BINARY column_name = '{col}'""")
        query_template = query_template.strip()
        datatype_query = query_template.format(tab=column.table.full_name, col=column.name)
        self._db.cursor().execute(datatype_query)
        result_set = self._db.cursor().fetchone()
        return result_set[0]
    
    def _fetch_indexes(self, table: base.TableReference) -> dict[str, bool]:

        index_query = textwrap.dedent(f""" SELECT COLUMN_NAME AS attname,  CASE WHEN COLUMN_KEY = 'PRI' THEN 'true' ELSE 'false' END AS indisprimary 
                                           FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.full_name}' 
                                           AND (COLUMN_NAME IN (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS 
                                           WHERE  TABLE_NAME = '{table.full_name}'
                                           AND INDEX_NAME = 'PRIMARY') OR COLUMN_KEY = 'MUL'); """)
        index_query = index_query.strip()
        
        cursor = self._db.cursor()
        cursor.execute(index_query)
        result_set = cursor.fetchall()
        index_map = {}
        for row in result_set:
            col_name, has_secondary_index = row
            index_map[col_name] = has_secondary_index == 'true'
        return index_map
    
    def _fetch_columns(self, table: base.TableReference) -> list[str]:
        """Retrieves all physical columns for a given table from the MYSQL metadata catalogs."""
        query_template = "SELECT column_name FROM information_schema.columns WHERE BINARY table_name = %s"
        self._db.cursor().execute(query_template, (table.full_name,))
        result_set = self._db.cursor().fetchall()
        return [col[0] for col in result_set]
    

 

class mysqlStatisticsInterface(db.DatabaseStatistics):
    """Statistics-specific parts of the MYSQL interface."""

    def __init__(self, mysql_db: mysqlInterface) -> None:
        super().__init__(mysql_db)    
    
    def _retrieve_total_rows_from_stats(self, table: base.TableReference) -> int:
        count_query = f"SELECT TABLE_ROWS  FROM INFORMATION_SCHEMA.TABLES WHERE BINARY TABLE_NAME = '{table.full_name}'"
        self._db.cursor().execute(count_query)
        count = self._db.cursor().fetchone()[0]
        return count
    
    def _retrieve_distinct_values_from_stats(self, column: base.ColumnReference) -> int:
        dist_query = "SELECT DISTINCTROW CARDINALITY FROM information_schema.statistics WHERE BINARY table_name = %s AND BINARY column_name = %s"
        self._db.cursor().execute(dist_query, (column.table.full_name, column.name))
        dist_values = self._db.cursor().fetchone()[0]
        return dist_values
        
    
    def _retrieve_min_max_values_from_stats(self, column: base.ColumnReference) -> tuple:
        if not self.enable_emulation_fallback:
            raise db.UnsupportedDatabaseFeatureError(self._db, "min/max value statistics")
        return self._calculate_min_max_values(column, cache_enabled=True)
    
    def _retrieve_most_common_values_from_stats(self, column: base.ColumnReference, k: int) -> list:
        return self._calculate_most_common_values(column, k)
    

    

    
    
    
def connect(*, name: str = "mysql", connect_string: str | None = None, config_file: str | None = ".mysql_connection", cache_enabled: bool = True, private: bool = False) -> mysqlInterface:
   
    db_pool = db.DatabasePool.get_instance()
    if config_file and not connect_string:
        if not os.path.exists(config_file):
            raise ValueError("Config file was given, but does not exist: " + config_file)
        with open(config_file, "r") as f:
            connect_string = f.readline().strip()
    elif not connect_string:
        raise ValueError("Connect string or config file are required to connect to MYSQL")

    mysql_db = mysqlInterface(connect_string, system_name=name, cache_enabled=cache_enabled)
    if not private:
        db_pool.register_database(name, mysql_db)
    
    return mysql_db
    
