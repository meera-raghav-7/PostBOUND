"""Models the different types of JOIN statements."""
from __future__ import annotations

import abc
import enum

from typing import Iterable

from postbound.qal import base, predicates as preds, qal, expressions as expr
from postbound.util import collections as collection_utils


class JoinType(enum.Enum):
    """Indicates the actual JOIN type, e.g. OUTER JOIN or NATURAL JOIN."""
    InnerJoin = "JOIN"
    OuterJoin = "OUTER JOIN"
    LeftJoin = "LEFT JOIN"
    RightJoin = "RIGHT JOIN"
    CrossJoin = "CROSS JOIN"

    NaturalInnerJoin = "NATURAL JOIN"
    NaturalOuterJoin = "NATURAL OUTER JOIN"
    NaturalLeftJoin = "NATURAL LEFT JOIN"
    NaturalRightJoin = "NATURAL RIGHT JOIN"

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.value


class Join(abc.ABC):
    """Abstract interface shared by all JOIN statements."""

    def __init__(self, join_type: JoinType, join_condition: preds.AbstractPredicate | None = None) -> None:
        self.join_type = join_type
        self.join_condition = join_condition

    @abc.abstractmethod
    def is_subquery_join(self) -> bool:
        """Checks, whether the JOIN statement is a table join or a subquery join."""
        raise NotImplementedError

    @abc.abstractmethod
    def columns(self) -> set[base.ColumnReference]:
        """Provides all columns that are referenced in this join."""
        raise NotImplementedError

    @abc.abstractmethod
    def tables(self) -> set[base.TableReference]:
        """Provides all tables that are referenced in this join."""
        raise NotImplementedError

    @abc.abstractmethod
    def iterexpressions(self) -> Iterable[expr.SqlExpression]:
        """Provides access to all directly contained expressions in this join.

        Nested expressions can be accessed from these expressions in a recursive manner (see the `SqlExpression`
        interface for details).
        """
        raise NotImplementedError

    @abc.abstractmethod
    def itercolumns(self) -> Iterable[base.ColumnReference]:
        """Provides access to all column in this joins.

        In contrast to the `columns` method, duplicates are returned multiple times, i.e. if a column is referenced `n`
        times in this join, it will also be returned `n` times by this method. Furthermore, the order in which
        columns are provided by the iterable matches the order in which they appear in this join.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return str(self)

    @abc.abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError


class TableJoin(Join):
    """JOIN for a target table, e.g. SELECT * FROM R JOIN S ON R.a = S.b."""

    @staticmethod
    def inner(joined_table: base.TableReference, join_condition: preds.AbstractPredicate | None = None) -> TableJoin:
        """Constructs an INNER JOIN with the given subquery."""
        return TableJoin(JoinType.InnerJoin, joined_table, join_condition)

    def __init__(self, join_type: JoinType, joined_table: base.TableReference,
                 join_condition: preds.AbstractPredicate | None = None) -> None:
        super().__init__(join_type, join_condition)
        self.joined_table = joined_table

    def is_subquery_join(self) -> bool:
        return False

    def tables(self) -> set[base.TableReference]:
        return {self.joined_table} | self.join_condition.tables()  # include joined_table just to be safe

    def columns(self) -> set[base.ColumnReference]:
        return self.join_condition.columns()

    def iterexpressions(self) -> Iterable[expr.SqlExpression]:
        return self.join_condition.iterexpressions()

    def itercolumns(self) -> Iterable[base.ColumnReference]:
        return self.join_condition.itercolumns()

    def __str__(self) -> str:
        join_str = str(self.join_type)
        join_prefix = f"{join_str} {self.joined_table}"
        if self.join_condition:
            condition_str = (f"({self.join_condition})" if self.join_condition.is_compound()
                             else str(self.join_condition))
            return join_prefix + f" ON {condition_str}"
        else:
            return join_prefix


class SubqueryJoin(Join):
    @staticmethod
    def inner(subquery: qal.SqlQuery, alias: str = "",
              join_condition: preds.AbstractPredicate | None = None) -> SubqueryJoin:
        """Constructs an INNER JOIN with the given subquery."""
        return SubqueryJoin(JoinType.InnerJoin, subquery, alias, join_condition)

    def __init__(self, join_type: JoinType, subquery: qal.SqlQuery, alias: str = "",
                 join_condition: preds.AbstractPredicate | None = None) -> None:
        super().__init__(join_type, join_condition)
        self.subquery = subquery
        self.alias = alias

    def is_subquery_join(self) -> bool:
        return True

    def tables(self) -> set[base.TableReference]:
        return self.subquery.tables() | self.join_condition.tables()

    def columns(self) -> set[base.ColumnReference]:
        return self.subquery.columns() | self.join_condition.columns()

    def iterexpressions(self) -> Iterable[expr.SqlExpression]:
        return collection_utils.flatten([self.subquery.iterexpressions(), self.join_condition.iterexpressions()])

    def itercolumns(self) -> Iterable[base.ColumnReference]:
        return collection_utils.flatten([self.subquery.itercolumns(), self.join_condition.itercolumns()])

    def __str__(self) -> str:
        join_type_str = str(self.join_type)
        join_str = f"{join_type_str} ({self.subquery})"
        if self.alias:
            join_str += f" AS {self.alias}"
        if self.join_condition:
            condition_str = (f"({self.join_condition})" if self.join_condition.is_compound()
                             else str(self.join_condition))
            join_str += f" ON {condition_str}"
        return join_str
