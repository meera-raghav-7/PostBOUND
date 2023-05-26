from __future__ import annotations

import collections
import math
from collections.abc import Iterable, Sequence
from typing import Optional

from postbound.qal import qal, base, predicates
from postbound.db import db
from postbound.optimizer import jointree
from postbound.optimizer.physops import selection as opsel, operators as physops
from postbound.util import collections as collection_utils, dicts as dict_utils


def _iterate_join_tree(current_node: jointree.AbstractJoinTreeNode) -> Sequence[jointree.IntermediateJoinNode]:
    if isinstance(current_node, jointree.BaseTableNode):
        return []
    assert isinstance(current_node, jointree.IntermediateJoinNode)
    return list(_iterate_join_tree(current_node.right_child)) + [current_node]


def _iterate_query_plan(current_node: db.QueryExecutionPlan) -> Sequence[db.QueryExecutionPlan]:
    if current_node.is_scan:
        return []
    if not current_node.is_join:
        assert len(current_node.children) == 1
        return _iterate_query_plan(current_node.children[0])
    right_child = current_node.inner_child if current_node.inner_child else current_node.children[1]
    return list(_iterate_query_plan(right_child)) + [current_node]


class QepsIdentifier:
    def __init__(self, tables: base.TableReference | Iterable[base.TableReference],
                 filter_predicate: Optional[predicates.AbstractPredicate] = None) -> None:
        self._tables = frozenset(collection_utils.enlist(tables))
        self._filter_predicate = filter_predicate
        self._hash_val = hash((self._tables, self._filter_predicate))

    @property
    def table(self) -> Optional[base.TableReference]:
        if not len(self._tables) == 1:
            return None
        return collection_utils.get_any(self._tables)

    @property
    def tables(self) -> frozenset[base.TableReference]:
        return self._tables

    @property
    def filter_predicate(self) -> Optional[predicates.AbstractPredicate]:
        return self._filter_predicate

    def __hash__(self) -> int:
        return self._hash_val

    def __eq__(self, other: object) -> bool:
        return (isinstance(other, type(self)) and self.tables == other.tables
                and self.filter_predicate == other.filter_predicate)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        table_str = (self.table.identifier() if len(self.tables) == 1
                     else "#" + "#".join(tab.identifier() for tab in self.tables))
        filter_str = f"[{self.filter_predicate}]" if self.filter_predicate else ""
        return table_str + filter_str


class QepsNode:
    def __init__(self, filter_aware: bool, gamma: float) -> None:
        self.filter_aware = filter_aware
        self.gamma = gamma
        self.operator_costs: dict[physops.JoinOperators, float] = collections.defaultdict(float)
        self.child_nodes: dict[QepsIdentifier, QepsNode] = collections.defaultdict(self._init_qeps)
        self._subquery_root: Optional[QepsNode] = None  # only used for subquery nodes

    @property
    def subquery_root(self) -> QepsNode:
        if not self._subquery_root:
            self._subquery_root = QepsNode(self.filter_aware, self.gamma)
        return self._subquery_root

    def recommend_operators(self, query: qal.SqlQuery, join_order: Sequence[jointree.IntermediateJoinNode],
                            current_assignment: physops.PhysicalOperatorAssignment) -> None:
        if not join_order:
            return

        next_join, *remaining_joins = join_order
        recommendation = self.current_recommendation()
        if recommendation:
            current_assignment.set_join_operator(physops.JoinOperatorAssignment(recommendation, next_join.tables()))

        next_node: jointree.AbstractJoinTreeNode = next_join.left_child
        if next_node.is_base_table_node():
            assert isinstance(next_node, jointree.BaseTableNode)
            child_node = self.child_nodes[self._make_identifier(query, next_node.table)]
            child_node.recommend_operators(query, remaining_joins, current_assignment)
        elif next_node.is_join_node():
            assert isinstance(next_node, jointree.IntermediateJoinNode)
            subquery_node = self.child_nodes[QepsIdentifier(next_node.tables())]
            subquery_node.subquery_recommendation(query, next_node, current_assignment)
            subquery_node.recommend_operators(query, remaining_joins, current_assignment)

    def integrate_costs(self, query: qal.SqlQuery, query_plan: Sequence[db.QueryExecutionPlan]) -> None:
        if not query_plan:
            return

        next_node, *remaining_nodes = query_plan
        if not next_node.is_join:
            self.integrate_costs(query, remaining_nodes)

        child_node: db.QueryExecutionPlan = next_node.outer_child if next_node.outer_child else next_node.children[0]
        if child_node.is_scan:
            self._integrate_base_join_costs(query, next_node, remaining_nodes)
        elif child_node.is_join:
            self._integrate_subquery_join_costs(query, next_node, remaining_nodes)

    def subquery_recommendation(self, query: qal.SqlQuery, node: jointree.IntermediateJoinNode,
                                current_assignment: physops.PhysicalOperatorAssignment) -> None:
        self.subquery_root.recommend_operators(query, _iterate_join_tree(node), current_assignment)

    def integrate_subquery_costs(self, query: qal.SqlQuery, query_plan: db.QueryExecutionPlan):
        self.subquery_root.integrate_costs(query, _iterate_query_plan(query_plan))

    def current_recommendation(self) -> Optional[physops.JoinOperators]:
        return dict_utils.argmin(self.operator_costs) if len(self.operator_costs) > 1 else None

    def update_costs(self, operator: physops.JoinOperators, cost: float) -> None:
        current_cost = self.operator_costs[operator]
        self.operator_costs[operator] = cost + self.gamma * current_cost

    def inspect(self, *, _current_indentation: int = 0) -> str:
        if not _current_indentation:
            return "[ROOT]\n" + self._child_inspect(2)

        prefix = " " * _current_indentation

        cost_str = "[" + ", ".join(f"{operator.value}={cost}" for operator, cost in self.operator_costs.items()) + "]"
        subquery_content = (self.subquery_root.inspect(_current_indentation=_current_indentation)
                            if self.subquery_root else "")
        child_str = self._child_inspect(_current_indentation)

        inspect_entries = [prefix + cost_str, prefix + subquery_content if subquery_content else "", child_str]
        return "\n".join(entry for entry in inspect_entries if entry)

    def _init_qeps(self) -> QepsNode:
        return QepsNode(self.filter_aware, self.gamma)

    def _make_identifier(self, query: qal.SqlQuery, table: base.TableReference) -> QepsIdentifier:
        filter_predicate = query.predicates().filters_for(table) if self.filter_aware else None
        return QepsIdentifier(table, filter_predicate)

    def _integrate_base_join_costs(self, query: qal.SqlQuery, current_node: db.QueryExecutionPlan,
                                   remaining_nodes: Sequence[db.QueryExecutionPlan]) -> None:
        if not current_node.table or not current_node.physical_operator or math.isnan(current_node.cost):
            raise ValueError("Plan node for QEP-S update must contain table, operator and cost")
        child_identifier = self._make_identifier(query, current_node.table)
        child_node = self.child_nodes[child_identifier]
        child_node.update_costs(current_node.physical_operator, current_node.cost)
        child_node.integrate_costs(query, remaining_nodes)

    def _integrate_subquery_join_costs(self, query: qal.SqlQuery, current_node: db.QueryExecutionPlan,
                                       remaining_nodes: Sequence[db.QueryExecutionPlan]) -> None:
        if not current_node.table or not current_node.physical_operator or math.isnan(current_node.cost):
            raise ValueError("Plan node for QEP-S update must contain table, operator and cost")
        subquery_plan_node = current_node.outer_child if current_node.outer_child else current_node.children[0]
        subquery_qeps_node = self.child_nodes[QepsIdentifier(subquery_plan_node.tables())]
        subquery_qeps_node.integrate_subquery_costs(query, subquery_plan_node)
        subquery_qeps_node.integrate_costs(query, remaining_nodes)

    def _child_inspect(self, indentation: int) -> str:
        prefix = " " * indentation
        child_content = []
        for identifier, child_node in self.child_nodes.items():
            child_inspect = child_node.inspect(_current_indentation=indentation + 2)
            child_content.append(f"{prefix}QEP-S node {identifier}\n{child_inspect}")
        return f"{prefix}-----\n".join(child for child in child_content)

    def __bool__(self) -> bool:
        return len(self.child_nodes) > 0 or len(self.operator_costs) > 0


class QueryExecutionPlanSynopsis:
    @staticmethod
    def create(filter_aware: bool, gamma: float) -> QueryExecutionPlanSynopsis:
        root = QepsNode(filter_aware, gamma)
        return QueryExecutionPlanSynopsis(root)

    def __init__(self, root: QepsNode) -> None:
        self.root = root

    def recommend_operators(self, query: qal.SqlQuery,
                            join_order: jointree.JoinTree) -> physops.PhysicalOperatorAssignment:
        current_assignment = (join_order.physical_operators() if isinstance(join_order, jointree.PhysicalQueryPlan)
                              else physops.PhysicalOperatorAssignment())
        self.root.recommend_operators(query, _iterate_join_tree(join_order.root), current_assignment)
        return current_assignment

    def integrate_costs(self, query: qal.SqlQuery, query_plan: db.QueryExecutionPlan) -> None:
        self.root.integrate_costs(query, _iterate_query_plan(query_plan))

    def inspect(self) -> str:
        return self.root.inspect()


def make_qeps(path: Iterable[base.TableReference], root: Optional[QepsNode] = None, *, gamma: float = 0.8) -> QepsNode:
    current_node = root if root is not None else QepsNode(False, gamma)
    root = current_node
    for table in path:
        current_node = current_node.child_nodes[QepsIdentifier(table)]
    return root

class TonicOperatorSelection(opsel.PhysicalOperatorSelection):

    def __init__(self, filter_aware: bool = False, gamma: float = 0.8, *,
                 database: Optional[db.Database] = None) -> None:
        super().__init__()
        self.filter_aware = filter_aware
        self.gamma = gamma
        self.qeps = QueryExecutionPlanSynopsis.create(filter_aware, gamma)
        self._db = database if database else db.DatabasePool.get_instance().current_database()

    def integrate_cost(self, query: qal.SqlQuery, query_plan: Optional[db.QueryExecutionPlan] = None) -> None:
        query_plan = self._db.optimizer().query_plan(query) if query_plan is None else query_plan
        self.qeps.integrate_costs(query, query_plan)

    def simulate_feedback(self, query: qal.SqlQuery) -> None:
        analyze_plan = self._db.optimizer().analyze_plan(query)
        self.incorporate_feedback(query, analyze_plan)

    def incorporate_feedback(self, query: qal.SqlQuery, analyze_plan: db.QueryExecutionPlan):
        if not analyze_plan.is_analyze():
            raise ValueError("Analyze plan required, but normal plan received")
        physical_qep = jointree.PhysicalQueryPlan.load_from_query_plan(analyze_plan, query)
        hinted_query = self._db.hinting().generate_hints(query, physical_qep)
        self.integrate_cost(hinted_query)

    def _apply_selection(self, query: qal.SqlQuery,
                         join_order: Optional[jointree.LogicalJoinTree | jointree.PhysicalQueryPlan]
                         ) -> physops.PhysicalOperatorAssignment:
        if not join_order or join_order.is_empty():
            join_order = self._obtain_native_join_order(query)
        return self.qeps.recommend_operators(query, join_order)

    def _description(self) -> dict:
        return {"name": "tonic", "filter_aware": self.filter_aware, "gamma": self.gamma}

    def _obtain_native_join_order(self, query: qal.SqlQuery) -> jointree.LogicalJoinTree:
        native_plan = self._db.optimizer().query_plan(query)
        return jointree.LogicalJoinTree.load_from_query_plan(native_plan, query)
