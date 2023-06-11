import unittest
import sys
sys.path.append('../..')
from postbound.db import mysql
from postbound.db import postgres
from postbound.qal import base
import configparser
import threading



class TestDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # Get the database connection details from the config file
         
        config = configparser.ConfigParser()
        config.read('config.ini')
        database_config_postgres = config['DATABASEPOSTGRES']
        conn_string_postgres = f"dbname={database_config_postgres['database']} user={database_config_postgres['username']} password={database_config_postgres['password']} host={database_config_postgres['host']} port={database_config_postgres['port']}"
        self.pg_connection = postgres.connect(connect_string=conn_string_postgres)
        self.pg_cursor = self.pg_connection.cursor()

        tables_config = config['TABLES']
        columns_config = config['COLUMNS']

        database_config_mysql = config['DATABASEMYSQL']
        mysql_host = database_config_mysql['host']
        mysql_user = database_config_mysql['username']
        mysql_database = database_config_mysql['database']
        mysql_password = database_config_mysql['password']
        conn_string_mysql = {
            "host": mysql_host,
            "user": mysql_user,
            "password": mysql_password,
            "database": mysql_database,
        }
        self.mysql_connection = mysql.connect(connect_string=conn_string_mysql)
        self.mysql_cursor = self.mysql_connection.cursor()

        self.table_ref1 = base.TableReference(tables_config['table1'])
        self.column_ref1 = base.ColumnReference(columns_config['table1_column1'], table=self.table_ref1)
        self.column_ref2 = base.ColumnReference(columns_config['table1_column2'], table=self.table_ref1)
        self.table_ref2 = base.TableReference(tables_config['table2'])
        self.column_ref3 = base.ColumnReference(columns_config['table2_column1'], table=self.table_ref2)
        self.column_ref4 = base.ColumnReference(columns_config['table2_column2'], table=self.table_ref2)

    def test_table_primary_key(self):
        result_postgres = self.pg_connection.schema().is_primary_key(self.column_ref1)
        self.assertTrue(result_postgres, msg="Column should be a primary key.")

        result_mysql = self.mysql_connection.schema().is_primary_key(self.column_ref1)
        self.assertTrue(result_mysql, msg="Column should be a primary key.")

    def test_secondary_index(self):
        result_postgres = self.pg_connection.schema().has_secondary_index(self.column_ref2)
        self.assertTrue(result_postgres, msg="Attribute is not indexed.")

        result_mysql = self.mysql_connection.schema().has_secondary_index(self.column_ref2)
        self.assertTrue(result_mysql, msg="Attribute is not indexed.") 

    def test_retrieve_total_rows_from_stats(self):
        result_postgres = self.pg_connection.statistics()._retrieve_total_rows_from_stats(self.table_ref1)
        self.assertGreater(result_postgres, 0, "No total rows found in PostgreSQL table")

        result_mysql = self.mysql_connection.statistics()._retrieve_total_rows_from_stats(self.table_ref1)
        self.assertGreater(result_mysql, 0, "No total rows found in MYSQL table")

    def test_retrieve_distinct_values_from_stats(self):
        result_postgres = self.pg_connection.statistics()._retrieve_distinct_values_from_stats(self.column_ref1)
        self.assertGreater(result_postgres, 0, "No total values found in PostgreSQL table")

        result_mysql = self.mysql_connection.statistics()._retrieve_distinct_values_from_stats(self.column_ref1)
        self.assertGreater(result_mysql, 0, "No total values found in MYSQL table")
    
    def test_index_presence(self):
        result_postgres = self.pg_connection.schema().has_index(self.column_ref1)
        self.assertTrue(result_postgres, msg="Attribute is not indexed.")

        result_mysql = self.mysql_connection.schema().has_index(self.column_ref1)
        self.assertTrue(result_mysql, msg="Attribute is not indexed.") 
        
    @classmethod
    def tearDownClass(self):
        self.pg_cursor.close()
        self.pg_connection.close()
        self.mysql_cursor.close()
        self.mysql_connection.close()
