from postbound.db import postgres
from postbound.qal import qal, base, clauses, transform, formatter
from postbound.optimizer import jointree
from postbound.optimizer.physops import operators as physops
from postbound.optimizer.planmeta import hints as planmeta


conn_string = "dbname=imdbload user=postgres password=root host=127.0.0.1 port=5432"

postgres_db = postgres.connect(connect_string=conn_string)
hint_service = postgres_db.hinting()

table1 = base.TableReference('imdbload', 'movie_info')
table2 = base.TableReference('imdbload', 'movie_keyword')
table3 = base.TableReference('imdbload', 'keyword')

# Create the PhysicalQueryPlan objects
tree1 = jointree.PhysicalQueryPlan.for_base_table(table= table1)
tree2 = jointree.PhysicalQueryPlan.for_base_table(table= table2)
tree3 = jointree.PhysicalQueryPlan.for_base_table(table= table3)

query_plan = jointree.PhysicalQueryPlan.cross_product_of(tree1, tree2, tree3)


result1 = jointree.LogicalJoinTree.for_base_table(table= table1)
result2 = jointree.LogicalJoinTree.for_base_table(table= table2)

joined_tree = jointree.LogicalJoinTree.joining(result1, result2)

assignment  = physops.PhysicalOperatorAssignment()

scan_operator = physops.ScanOperatorAssignment(table=table2, operator=physops.ScanOperators.IndexOnlyScan)

# Call the set_scan_operator function with the scan_operator object
my_result = assignment.set_scan_operator(scan_operator)
#join_operator = physops.JoinOperators.BlockNestedLoopJoin
#scan_operator = physops.ScanOperators.SequentialScan
#result = planmeta.HintType.JoinDirectionHint
#supports_hint = hint_service.supports_hint(join_operator)
#print(supports_hint)

plan_parameters = planmeta.PlanParameterization()

# Call the add_cardinality_hint function with the appropriate arguments
tables = [table1, table2, table3]
cardinality = 1000

plan = plan_parameters.add_cardinality_hint(tables , cardinality)

table_ref = base.TableReference('company_name')
column_ref = base.ColumnReference('id', table=table_ref)

select_clause = clauses.Select(targets=[clauses.BaseProjection.count_star()])
from_clause = clauses.ImplicitFromClause(tables=[base.TableReference("role_type")])

query = qal.ImplicitSqlQuery(select_clause=select_clause,from_clause=from_clause)


adapted_query = hint_service.generate_hints(query=query, join_order=query_plan, plan_parameters=plan_parameters,physical_operators=assignment)

print(adapted_query)
# Print the adapted query


#formatted_query = hint_service.format_query(query)


# Create a qal.SqlQuery object
#query = qal.ImplicitSqlQuery()
#query.select_clause(clauses.SelectType.Select, "id")




# Format the query using the hint service


#result5 = postgres_db.statistics()._retrieve_total_rows_from_stats(table_ref)
#result6 = postgres_db.statistics()._retrieve_distinct_values_from_stats(column_ref)
#result7 = postgres_db.statistics()._retrieve_min_max_values_from_stats(column_ref)
#result8 = postgres_db.statistics()._retrieve_most_common_values_from_stats(column_ref,100)
#result1 = postgres_db.database_name()


#result1 = postgres_db.schema().lookup_column(column_ref, [table_ref])
#result2 = postgres_db.schema().is_primary_key(column_ref)
#result3 = postgres_db.schema().has_index(column_ref)
#result4 = postgres_db.schema().datatype(column_ref)






