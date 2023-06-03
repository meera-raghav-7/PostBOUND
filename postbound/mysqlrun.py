from postbound.db import mysql
from postbound.qal import qal, base, clauses, transform, formatter
from postbound.optimizer import jointree
from postbound.optimizer.physops import operators as physops
from postbound.optimizer.planmeta import hints as planmeta
conn_string = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "1234",
    "database": "imdbload",
    
}

mysql_db = mysql_db = mysql.connect(connection_args=mysql.MysqlConnectionArguments(**conn_string))




hint_service = mysql_db.hinting()
table1 = base.TableReference('imdbload', 'movie_info')
table2 = base.TableReference('imdbload', 'movie_keyword')
table3 = base.TableReference('imdbload', 'keyword')
hint_service.tables = [table1, table2, table3]
# Create the PhysicalQueryPlan objects
tree1 = jointree.PhysicalQueryPlan.for_base_table(table= table1)
tree2 = jointree.PhysicalQueryPlan.for_base_table(table= table2)
tree3 = jointree.PhysicalQueryPlan.for_base_table(table= table3)

query_plan = jointree.PhysicalQueryPlan.cross_product_of(tree1, tree2, tree3)

assignment  = physops.PhysicalOperatorAssignment()
# Create a JoinOperatorAssignment with the join information
tables = [table1, table2, table3]
join_operator_assignment = physops.JoinOperatorAssignment(join=tables, operator=physops.JoinOperators.BlockNestedLoopJoin)

#join_operator = physops.JoinOperatorAssignment(table=table2, operator=physops.JoinOperators.BlockNestedLoopJoin)

# Call the set_scan_operator function with the scan_operator object
my_result = assignment.set_join_operator(join_operator_assignment)

result1 = jointree.LogicalJoinTree.for_base_table(table= table1)
result2 = jointree.LogicalJoinTree.for_base_table(table= table2)

joined_tree = jointree.LogicalJoinTree.joining(result1, result2)

select_clause = clauses.Select(targets=[clauses.BaseProjection.count_star()])
from_clause = clauses.ImplicitFromClause(tables=[base.TableReference("role_type")])

query = qal.ImplicitSqlQuery(select_clause=select_clause,from_clause=from_clause)

#scan_operator = physops.ScanOperators.SequentialScan
supports_hint = hint_service.supports_hint(join_operator_assignment)



select_clause = clauses.Select(targets=[clauses.BaseProjection.count_star()])
from_clause = clauses.ImplicitFromClause(tables=[base.TableReference("role_type")])

query = qal.ImplicitSqlQuery(select_clause=select_clause,from_clause=from_clause)


plan_parameters = planmeta.PlanParameterization()
#plan_parameters.add_join_order_hint([table1, table2, table3])

table = base.TableReference('movie_info')

index_list = ["idx1", "idx2", "id_idx3" ]
#plan = plan_parameters.add_index_hint(table, index_list)
plan = plan_parameters.add_index_hint(table, index_list)


adapted_query = hint_service.generate_hints(
    query=query,
    join_order=query_plan, plan_parameters=plan_parameters,
    physical_operators=assignment
)


print(adapted_query)
#formatted_query = hint_service.format_query(query)
#print(formatted_query)

#table_ref = base.TableReference('company_name')
#column_ref = base.ColumnReference('id', table=table_ref)

#result1 = mysql_db.schema().lookup_column(column_ref, [table_ref])
#result2 = mysql_db.schema().is_primary_key(column_ref)
#result3 = mysql_db.schema().has_secondary_index(column_ref)
#result4 = mysql_db.schema().datatype(column_ref)
#result5 = mysql_db.statistics()._retrieve_total_rows_from_stats(table_ref)
#result6 = mysql_db.statistics()._retrieve_distinct_values_from_stats(column_ref)
#result7 = mysql_db.statistics()._retrieve_min_max_values_from_stats(column_ref)

#result8 = mysql_db.statistics()._retrieve_most_common_values_from_stats(column_ref,20)








