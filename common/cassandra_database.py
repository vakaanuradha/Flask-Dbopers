from .database import Database
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv

class CassandraDatabase(Database):
    def connection(self, username, password):
        cloud_config = {
            'secure_connect_bundle': 'C:\\Users\\anuradha.manubothu\\Downloads\\secure-connect-firstdb.zip'
        }
        auth_provider = PlainTextAuthProvider(username, password)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        return session

    def create_schema(self, host, username, password, schema_name):
        try:
            session = self.connection(username,password)
            row = session.execute(schema_name).one()
            return f"Schema {schema_name} created successfully"
        except Exception as er:
            return str(er)

    def drop_schema(self, host, username, password, schema_name):
        pass

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            session = self.connection(username,password)
            column_names = "(" + ", ".join(["{} {}".format(k, v) for k, v in columns.items()]) + ")"
            query = f"CREATE TABLE {schema_name}.{table_name}{column_names}"
            print(query)
            session.execute(query)
            return f"table {table_name} created successfully"
        except Exception as er:
            return str(er)

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            session = self.connection(username,password)
            query = f"DROP TABLE {schema_name}.{table_name}"
            print(query)
            session.execute(query)
            return f"table {table_name} dropped successfully"
        except Exception as er:
            return str(er)

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            session = self.connection(username,password)
            column_names = "(" + ", ".join(columns.keys()) + ")"
            value_s = "(" + ", ".join(["%s" for key in columns.keys()]) + ")"
            query = f"INSERT INTO {schema_name}.{table_name} {column_names} VALUES {value_s}"
            values = tuple(columns.values())
            session.execute(query,values)
            return f"data added successfully"
        except Exception as er:
            return str(er)

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            session = self.connection(username,password)
            column_names = "(" + ", ".join(columns) + ")"
            value_s = "(" + ", ".join(["%s" for key in columns]) + ")"
            query = f"INSERT INTO {schema_name}.{table_name} {column_names} VALUES {value_s}"
            with open(input_file_name) as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    values = tuple(row)
                    session.execute(query,values)
            return f"data added successfully"
        except Exception as er:
            return str(er)

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            session = self.connection(username,password)
            updated_values = ", ".join([f"{k} = '{v}'" for k, v in updated_values.items()])
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"UPDATE {schema_name}.{table_name} SET {updated_values} WHERE {filter_values}"
            session.execute(query)
            return f"data updated successfully"
        except Exception as er:
            return str(er)

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            session = self.connection(username,password)
            query = f"SELECT *FROM {schema_name}.{table_name}"
            records = session.execute(query)
            # for record in records:
            #     print(record)
            output = [rec for rec in records]
            return output
        except Exception as er:
            return str(er)

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            session = self.connection(username,password)
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"SELECT *FROM {schema_name}.{table_name} WHERE {filter_values}"
            records = session.execute(query)
            output = []
            for record in records:
                print(record)
            #     output.append(record)
            # output = [rec for rec in records]
            return record
        except Exception as er:
            return str(er)

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            session = self.connection(username,password)
            filter_values = " and ".join([f"{k} = '{v}'" for k, v in filters.items()])
            query = f"DELETE FROM {schema_name}.{table_name} WHERE {filter_values}"
            session.execute(query)
            return f"data deleted successfully"
        except Exception as er:
            return str(er)

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            session = self.connection(username,password)
            query = f"DELETE FROM {schema_name}.{table_name}"
            print(query)
            session.execute(query)
            return f"data deleted successfully"
        except Exception as er:
            return str(er)

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            session = self.connection(username,password)
            query = f"TRUNCATE TABLE {schema_name}.{table_name}"
            session.execute(query)
            return f"table truncated successfully"
        except Exception as er:
            return str(er)