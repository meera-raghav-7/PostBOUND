
import collections
import enum
import math
import pprint
from typing import Any, Dict, FrozenSet, List, Callable, Iterable, Union

import numpy as np

from transform import db, mosp, ues, util


class QueryNode(enum.Enum):
    SeqScan = "SeqScan"
    IndexScan = "IndexScan"
    IndexOnlyScan = "IndexOnlyScan"
    NestLoop = "NestLoop"
    HashJoin = "HashJoin"
    SortMergeJoin = "MergeJoin"

    def is_join(self) -> bool:
        return self == QueryNode.NestLoop or self == QueryNode.HashJoin

    def is_scan(self) -> bool:
        return self in [QueryNode.SeqScan, QueryNode.IndexScan, QueryNode.IndexOnlyScan]

    def __str__(self) -> str:
        return self.value


def _join_id(join: Union[mosp.MospJoin, Iterable[db.TableRef]]) -> int:
    if isinstance(join, mosp.MospJoin):
        tables = join.collect_tables()
    else:
        tables = join
    return hash(frozenset(sorted(tables)))


class HintedMospQuery:
    """A HintedMospQuery augments SQL queries by PostgreSQL query hints. This assumes a fixed join order."""
    def __init__(self, query: mosp.MospQuery):
        self.query: mosp.MospQuery = query

        # Build the query join paths. A join path describes which joins have to be executed before a certain join
        # can run. This assumes a fixed join order. For convenience, each join path also contains the join in question
        # itself. I.e., The join path for joining table C could look like A B C, indicating that the join between
        # A and B has to executed first.
        self.join_paths: Dict[int, List[db.TableRef]] = dict()
        base_table = query.base_table()
        curr_join_path = [base_table]
        for join in query.joins():
            curr_join_path.extend(join.collect_tables())
            self.join_paths[_join_id(join)] = list(curr_join_path)  # copy the join path to prevent unintended updates

            # FIXME: this algorithm currently only works for 1 layer of subqueries
            # subqueries with subqueries are unsupported. A recursive algorithm should solve this problem quite nicely.
            if join.is_subquery():
                sq_base_table = join.base_table()
                sq_join_path = [sq_base_table]
                for sq_join in join.subquery.joins():
                    sq_join_path.extend(sq_join.collect_tables())
                    self.join_paths[_join_id(sq_join)] = list(sq_join_path)  # copy the join path once again

        self.scan_hints: Dict[db.TableRef, QueryNode] = dict()
        self.join_hints: Dict[int, QueryNode] = dict()
        self.cardinality_bounds: Dict[int, int] = dict()
        self.join_contents: Dict[int, mosp.MospJoin] = dict()
        self.bounds_stats: Dict[FrozenSet[db.TableRef], Dict[str, int]] = dict()
        self.pg_parameters: Dict[str, Any] = dict()

    def force_nestloop(self, join: Union[mosp.MospJoin, Iterable[db.TableRef]]) -> None:
        jid = _join_id(join)
        self.join_hints[jid] = QueryNode.NestLoop
        self.join_contents[jid] = join
        self._update_join_paths(jid, join)

    def force_hashjoin(self, join: Union[mosp.MospJoin, Iterable[db.TableRef]]) -> None:
        jid = _join_id(join)
        self.join_hints[jid] = QueryNode.HashJoin
        self.join_contents[jid] = join
        self._update_join_paths(jid, join)

    def force_mergejoin(self, join: Union[mosp.MospJoin, Iterable[db.TableRef]]) -> None:
        jid = _join_id(join)
        self.join_hints[jid] = QueryNode.SortMergeJoin
        self.join_contents[jid] = join
        self._update_join_paths(jid, join)

    def force_seqscan(self, table: db.TableRef) -> None:
        self.scan_hints[table] = QueryNode.SeqScan

    def force_idxscan(self, table: db.TableRef) -> None:
        # we can use an IndexOnlyScan here, b/c IndexOnlyScan falls back to IndexScan automatically if necessary
        self.scan_hints[table] = QueryNode.IndexOnlyScan

    def set_upperbound(self, join: Union[mosp.MospJoin, Iterable[db.TableRef]], nrows: int) -> None:
        jid = _join_id(join)
        self.cardinality_bounds[jid] = nrows
        self.join_contents[jid] = join
        self._update_join_paths(jid, join)

    def set_pg_param(self, parameter: Union[str, Dict[str, Any]] = None, value: Any = None, **kwargs) -> None:
        """Adds planner hints that influence the Postgres optimization behaviour for the entire query.

        These hints are supplied as Postgres parameters - see
        https://www.postgresql.org/docs/current/runtime-config-query.html for all options

        `set_pg_param` can be called in three different ways:

        - `set_pg_param("foo", "bar")` will only set the parameter `foo` to value `'bar'` and leave all other
        parameters untouched.
        - `set_pg_param(foo="bar")` will do the same as `set_pg_param("foo", "bar")`, but has a nicer syntax
        - `set_pg_param({foo: "bar"})` will replace all existing parameters and only use the parameters supplied in the
        dictionary.

        """
        if parameter is None:
            self.pg_parameters = util.dict_merge(self.pg_parameters, dict(kwargs))
        elif isinstance(parameter, dict):
            self.pg_parameters = dict(parameter)
        else:
            self.pg_parameters[parameter] = value

    def merge_with(self, other_query: "HintedMospQuery") -> None:
        self.scan_hints = util.dict_merge(self.scan_hints, other_query.scan_hints)
        self.join_hints = util.dict_merge(self.join_hints, other_query.join_hints)
        self.cardinality_bounds = util.dict_merge(self.cardinality_bounds, other_query.cardinality_bounds)
        self.join_contents = util.dict_merge(self.join_contents, other_query.join_contents)

    def store_bounds_stats(self, join: FrozenSet[db.TableRef], bounds: Dict[str, int]) -> None:
        self.bounds_stats[join] = bounds

    def generate_sqlcomment(self, *, strip_empty: bool = False) -> str:
        if strip_empty and not self.scan_hints and not self.join_hints and not self.cardinality_bounds:
            return ""

        scan_hints_stringified = "\n".join(self._scan_hint_to_str(tab) for tab in self.scan_hints.keys())
        join_hints_stringified = "\n".join(self._join_hint_to_str(join_id) for join_id in self.join_hints.keys())
        cardinality_bounds_stringified = "\n".join(self._cardinality_bound_to_str(join_id)
                                                   for join_id in self.cardinality_bounds.keys())

        pg_params = self._pg_params_to_str()
        hint = "\n".join(s for s in ["/*+",
                                     scan_hints_stringified, join_hints_stringified, cardinality_bounds_stringified,
                                     "*/"] if s)
        return "\n".join([pg_params, hint])

    def generate_query(self, *, strip_empty: bool = False) -> str:
        hint = self.generate_sqlcomment(strip_empty=strip_empty)
        return "\n".join([hint, self.query.text() + ";"])

    def _scan_hint_to_str(self, base_table: db.TableRef) -> str:
        operator = self.scan_hints[base_table]
        return f"{operator.value}({base_table.qualifier()})"

    def _join_hint_to_str(self, join_id: int) -> str:
        full_join_path = self._join_path_to_str(join_id)
        return f"{self.join_hints[join_id].value}({full_join_path})"

    def _cardinality_bound_to_str(self, join_id: int) -> str:
        full_join_path = self._join_path_to_str(join_id)
        n_rows = self.cardinality_bounds[join_id]
        return f"Rows({full_join_path} #{n_rows})"

    def _pg_params_to_str(self) -> str:
        param_template = "SET {param} = {value};"
        stringified_params = []
        for param, value in self.pg_parameters.items():
            stringified_value = f"'{value}'" if isinstance(value, str) else value
            stringified_params.append(param_template.format(param=param, value=stringified_value))
        return " ".join(stringified_params)

    def _join_path_to_str(self, join_id: int) -> str:
        return " ".join(tab.qualifier() for tab in self.join_paths[join_id])

    def _update_join_paths(self, join_id: int, join: Union[mosp.MospJoin, Iterable[db.TableRef]]) -> None:
        if isinstance(join, mosp.MospJoin):
            return
        self.join_paths[join_id] = join

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.generate_sqlcomment()


def idxnlj_subqueries(query: mosp.MospQuery, *, nestloop="first", idxscan="fk") -> HintedMospQuery:
    if idxscan not in ["pk", "fk"]:
        raise ValueError("idxscan must be either 'pk' or 'fk', not '{}'".format(idxscan))
    if nestloop not in ["first", "all"]:
        raise ValueError("nestloop must be either 'first' or 'all', not '{}'".format(nestloop))

    hinted_query = HintedMospQuery(query)
    for sq in [sq.subquery for sq in query.subqueries()]:
        fk_table = sq.base_table()
        if idxscan == "fk":
            hinted_query.force_idxscan(fk_table)

        if nestloop == "first":
            first_pk_join = sq.joins()[0]
            hinted_query.force_nestloop(first_pk_join)

            if idxscan == "pk":
                pk_table = first_pk_join.base_table()
                hinted_query.force_idxscan(pk_table)
        elif nestloop == "all":
            for join_idx, join in enumerate(sq.joins()):
                hinted_query.force_nestloop(join)

                if idxscan == "pk" or join_idx > 0:
                    pk_table = join.base_table()
                    hinted_query.force_idxscan(pk_table)
    return hinted_query


def bound_hints(query: mosp.MospQuery, bounds_data: ues.BoundsTracker) -> HintedMospQuery:
    bounds_data.fill_missing_bounds(query=query)
    hinted_query = HintedMospQuery(query)
    visited_tables = [query.base_table()]
    for join in query.joins():
        if join.is_subquery():
            subquery_hints = bound_hints(join.subquery, bounds_data)
            hinted_query.merge_with(subquery_hints)
        visited_tables.extend(join.collect_tables())
        tables_key = frozenset(visited_tables)
        join_bounds = bounds_data.get(tables_key, None)
        if join_bounds:
            hinted_query.set_upperbound(join, join_bounds.upper_bound)
    return hinted_query


DEFAULT_IDXLOOKUP_PENALTY: float = 2.0
DEFAULT_HASHJOIN_PENALTY: float = 0.2


def operator_hints(query: mosp.MospQuery, bounds_data: ues.BoundsTracker, *,
                   hashjoin_penalty: float = DEFAULT_HASHJOIN_PENALTY,
                   indexlookup_penalty: float = DEFAULT_IDXLOOKUP_PENALTY,
                   hashjoin_estimator: Callable[[int, int], float] = None,
                   nlj_estimator: Callable[[int, int], float] = None,
                   stats_collector: Dict[str, int] = None,
                   verbose: bool = False) -> HintedMospQuery:
    indexlookup_penalty = DEFAULT_IDXLOOKUP_PENALTY if indexlookup_penalty is None else indexlookup_penalty
    hashjoin_penalty = DEFAULT_HASHJOIN_PENALTY if hashjoin_penalty is None else hashjoin_penalty
    hinted_query = HintedMospQuery(query)
    visited_tables = [query.base_table()]
    selection_stats = stats_collector if stats_collector is not None else collections.defaultdict(int)

    for join in query.joins():
        if join.is_subquery():
            subquery_hints = operator_hints(join.subquery, bounds_data)
            hinted_query.merge_with(subquery_hints)
        visited_tables.extend(join.collect_tables())
        tables_key = frozenset(visited_tables)
        bound_data = bounds_data.fetch_bound(visited_tables)
        if bound_data:
            upper_bound = bound_data.join_bound
            intermediate_bound, candidate_bound = bound_data.intermediate_bound, bound_data.candidate_bound

            # Choose the "optimal" operators. The formulas to estimate operator costs are _very_ _very_ coarse
            # grained and heuristic in nature. If more complex formulas are required, they can be supplied as arguments

            if nlj_estimator:
                nlj_cost = nlj_estimator(intermediate_bound, candidate_bound)
            else:
                # For NLJ we assume Index-NLJ and use its simplified formula:
                # The intermediate relation will become the outer loop and the new relation the inner loop. This is
                # because an intermediate relation will most likely not have an intact index. Further, we simply assume
                # that there exists an applicable index on the new relation (and are thus able to calculate an IdxNLJ
                # bound in the first place).
                idxlookup_cost = (1 + indexlookup_penalty) * intermediate_bound * math.log(candidate_bound)
                nlj_cost = intermediate_bound + idxlookup_cost

            if hashjoin_estimator:
                hashjoin_cost = hashjoin_estimator(intermediate_bound, candidate_bound)
            else:
                # For HashJoin we again use a simplified formula:
                # The smaller relation will be used to construct the hash table. Construction of the table is penalized
                # according to the hashjoin penalty. The larger relation will be used to perform the hash table
                # lookups. Hash table lookups are rather cheap but still not for free. Therefore we penalize usage
                # of the inner (larger) relation by 0.5 * penalty.
                min_bound = min(intermediate_bound, candidate_bound)
                max_bound = max(intermediate_bound, candidate_bound)
                hashjoin_cost = (1 + hashjoin_penalty) * min_bound + max_bound

            mergejoin_cost = np.inf  # don't consider Sort-Merge join for now

            if nlj_cost <= hashjoin_cost and nlj_cost <= mergejoin_cost:
                selection_stats["NLJ"] += 1
                hinted_query.force_nestloop(join)
            elif hashjoin_cost <= nlj_cost and hashjoin_cost <= mergejoin_cost:
                selection_stats["HashJoin"] += 1
                hinted_query.force_hashjoin(join)
            elif mergejoin_cost <= nlj_cost and mergejoin_cost <= hashjoin_cost:
                selection_stats["MergeJoin"] += 1
                hinted_query.force_mergejoin(join)
            else:
                raise util.StateError("The universe dissolves..")

            hinted_query.store_bounds_stats(tables_key, {"ues": upper_bound,
                                                         "nlj": nlj_cost,
                                                         "hashjoin": hashjoin_cost,
                                                         "mergejoin": mergejoin_cost})

    if verbose:
        pprint.pprint(dict(selection_stats))

    return hinted_query
