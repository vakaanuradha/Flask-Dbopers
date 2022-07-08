from .database import Database
import pymongo
import csv


class MongoDatabase(Database):
    def create_schema(self, host, username, password, schema_name):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            return f"Schema {schema_name} created successfully"
        except Exception as er:
            return str(er)

    def drop_schema(self, host, username, password, schema_name):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            #db = client[schema_name]
            client.drop_database(schema_name)
            #db.dropDatabase()
            return f"Schema {schema_name} deleted successfully"
        except Exception as er:
            return str(er)

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            return f"Table {table_name} created successfully"
        except Exception as er:
            return str(er)

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            collection.drop()
            return f"Table {table_name} deleted successfully"
        except Exception as er:
            return str(er)

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            collection.insert_one(columns)
            return f"Data added successfully"
        except Exception as er:
            return str(er)

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            rows_list = []
            with open(input_file_name) as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    row_dict = {}
                    counter = 0
                    for column in columns:
                        row_dict[column] = row[counter]
                        counter += 1
                    rows_list.append(row_dict)
            collection.insert_many(rows_list)
            return f"Records added successfully"
        except Exception as er:
            return str(er)

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            new_record = {"$set": updated_values}
            collection.update_many(filters, new_record)
            return f"Data updated successfully"
        except Exception as er:
            return str(er)

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            records = collection.find({}, {'_id': False})
            output = [rec for rec in records]
            return output
        except Exception as er:
            return str(er)

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            filter_dict = {k: {'$in': [v]} for k, v in filters.items()}
            print(filter_dict)
            records = collection.find(filter_dict, {'_id': False})
            output = [rec for rec in records]
            return output
        except Exception as er:
            return str(er)

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            filter_dict = {k: {'$in': [v]} for k, v in filters.items()}
            collection.delete_many(filter_dict)
            return "Records deleted successfully"
        except Exception as er:
            return str(er)

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            collection.delete_many({})
            return "Records deleted successfully"
        except Exception as er:
            return str(er)

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            client = pymongo.MongoClient(
                f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            )
            db = client[schema_name]
            collection = db[table_name]
            collection.remove()
            return "Records deleted successfully"
        except Exception as er:
            return str(er)