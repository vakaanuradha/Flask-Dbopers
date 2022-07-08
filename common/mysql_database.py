from .database import Database
import mysql.connector as conn
import csv


class MysqlDatabase(Database):
    def create_schema(self, host, username, password, schema_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True)
            cursor = mysql_db.cursor()
            query = f"CREATE DATABASE {schema_name}"
            cursor.execute(query)
            return f"Schema {schema_name} created successfully"
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

    def drop_schema(self, host, username, password, schema_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True)
            cursor = mysql_db.cursor()
            query = f"DROP DATABASE IF EXISTS {schema_name}"
            cursor.execute(query)
            return f"Schema {schema_name} dropped successfully"
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            print(mysql_db)
            cursor = mysql_db.cursor()
            column_names = "(" + ", ".join(["{} {}".format(k,v) for k,v in columns.items()]) + ")"
            query = f"CREATE TABLE if not exists {table_name} {column_names}"
            cursor.execute(query)
            return f"Table {table_name} created successfully"
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            query = f"DROP TABLE IF EXISTS {table_name}"
            cursor.execute(query)
            return f"Table {table_name} deleted successfully"
        except Exception as er:
            return str(er)

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            column_names = "(" + ", ".join(columns.keys()) + ")"
            value_s = "(" + ", ".join(["%s" for key in columns.keys()]) + ")"
            query = f"INSERT INTO {table_name} {column_names} VALUES {value_s}"
            values = tuple(columns.values())
            cursor.execute(query, values)
            mysql_db.commit()
            mysql_db.close()
            return f"Record added successfully"
        except Exception as er:
            return str(er)

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            print(columns)
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            rows_list = []
            with open(input_file_name) as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    rows_list.append(tuple(row))
            column_names = "(" + ", ".join(columns) + ")"
            value_s = "(" + ", ".join(["%s" for key in columns]) + ")"
            query = f"INSERT INTO {table_name} {column_names} VALUES {value_s}"
            print(query)
            print(rows_list)
            cursor.executemany(query, rows_list)
            mysql_db.commit()
            mysql_db.close()
            return f"Record added successfully"
        except Exception as er:
            return str(er)

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            updated_values = ", ".join([f"{k} = '{v}'" for k,v in updated_values.items()])
            filter_values = " and ".join([f"{k} = '{v}'" for k,v in filters.items()])
            query = f"UPDATE {table_name} SET {updated_values} WHERE {filter_values}"
            cursor.execute(query)
            mysql_db.commit()
            return f"Record updated successfully"
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            query = f"SELECT *FROM {table_name}"
            cursor.execute(query)
            records = cursor.fetchall()
            return records
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"SELECT *FROM {table_name} WHERE {filter_values}"
            cursor.execute(query)
            records = cursor.fetchall()
            return records
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"DELETE FROM {table_name} WHERE {filter_values}"
            cursor.execute(query)
            mysql_db.commit()
            return f"Records deleted from {table_name}"
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            query = f"DELETE FROM {table_name}"
            cursor.execute(query)
            mysql_db.commit()
            return f"Records deleted from {table_name}"
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            cursor = mysql_db.cursor()
            query = f"TRUNCATE TABLE {table_name}"
            cursor.execute(query)
            return f"Records deleted from {table_name}"
        except Exception as er:
            return str(er)
        finally:
            mysql_db.close()

