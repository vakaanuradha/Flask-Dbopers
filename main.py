import json

from flask import Flask, request, jsonify
from common.mysql_database import MysqlDatabase
from common.mongo_database import MongoDatabase
from common.cassandra_database import CassandraDatabase


app = Flask(__name__)
mysql_db = MysqlDatabase()
mongo_db = MongoDatabase()
cassandra_db = CassandraDatabase()


@app.route('/health-check')
def health_check():
    return "success"


@app.route('/mysql/create_schema', methods=['POST'])
def create_schema():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    result = mysql_db.create_schema(host_name,user_name,password,schema_name)
    return jsonify(result)


@app.route('/mysql/drop_schema', methods=['POST'])
def drop_schema():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    result = mysql_db.drop_schema(host_name,user_name,password,schema_name)
    return jsonify(result)


@app.route('/mysql/create_table', methods=['POST'])
def create_table():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns)
    result = mysql_db.create_table(host_name,user_name,password,schema_name,table_name,**columns)
    return jsonify(result)


@app.route('/mysql/drop_table', methods=['POST'])
def drop_table():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = mysql_db.drop_table(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/mysql/insert_record', methods=['POST'])
def insert_record():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns)
    result = mysql_db.insert_record(host_name,user_name,password,schema_name,table_name,**columns)
    return jsonify(result)


@app.route('/mysql/insert_multiple_records', methods=['POST'])
def insert_multiple_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns.replace("'",'"'))
    input_file_name = request.values.get('input_file_name')
    result = mysql_db.insert_multiple_records(host_name,user_name,password,schema_name,table_name,input_file_name,*columns)
    return jsonify(result)


@app.route('/mysql/update_records', methods=['POST'])
def update_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    updated_values = request.values.get('updated_values')
    updated_values = json.loads(updated_values)
    result = mysql_db.update_records(host_name,user_name,password,schema_name,table_name,filters, **updated_values)
    return jsonify(result)


@app.route('/mysql/retrieve_records', methods=['POST'])
def retrieve_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = mysql_db.retrieve_records(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/mysql/retrieve_records_with_filter', methods=['POST'])
def retrieve_records_with_filter():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    result = mysql_db.retrieve_records_with_filter(host_name,user_name,password,schema_name,table_name, **filters)
    return jsonify(result)


@app.route('/mysql/delete_records_with_filter', methods=['POST'])
def delete_records_with_filter():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    result = mysql_db.delete_records_with_filter(host_name,user_name,password,schema_name,table_name, **filters)
    return jsonify(result)


@app.route('/mysql/delete_records', methods=['POST'])
def delete_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = mysql_db.delete_records(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/mongo/create_schema', methods=['POST'])
def mongo_create_schema():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    result = mongo_db.create_schema(host_name,user_name,password,schema_name)
    return jsonify(result)


@app.route('/mongo/drop_schema', methods=['POST'])
def mongo_drop_schema():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    result = mongo_db.drop_schema(host_name,user_name,password,schema_name)
    return jsonify(result)


@app.route('/mongo/create_table', methods=['POST'])
def mongo_create_table():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns)
    result = mongo_db.create_table(host_name,user_name,password,schema_name,table_name,**columns)
    return jsonify(result)


@app.route('/mongo/drop_table', methods=['POST'])
def mongo_drop_table():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = mongo_db.drop_table(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/mongo/insert_record', methods=['POST'])
def mongo_insert_record():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns)
    result = mongo_db.insert_record(host_name,user_name,password,schema_name,table_name,**columns)
    return jsonify(result)


@app.route('/mongo/insert_multiple_records', methods=['POST'])
def mongo_insert_multiple_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns.replace("'",'"'))
    input_file_name = request.values.get('input_file_name')
    result = mongo_db.insert_multiple_records(host_name,user_name,password,schema_name,table_name,input_file_name,*columns)
    return jsonify(result)


@app.route('/mongo/update_records', methods=['POST'])
def mongo_update_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    updated_values = request.values.get('updated_values')
    updated_values = json.loads(updated_values)
    result = mongo_db.update_records(host_name,user_name,password,schema_name,table_name,filters, **updated_values)
    return jsonify(result)


@app.route('/mongo/retrieve_records', methods=['POST'])
def mongo_retrieve_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = mongo_db.retrieve_records(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/mongo/retrieve_records_with_filter', methods=['POST'])
def mongo_retrieve_records_with_filter():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    result = mongo_db.retrieve_records_with_filter(host_name,user_name,password,schema_name,table_name, **filters)
    return jsonify(result)


@app.route('/mongo/delete_records_with_filter', methods=['POST'])
def mongo_delete_records_with_filter():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    result = mongo_db.delete_records_with_filter(host_name,user_name,password,schema_name,table_name, **filters)
    return jsonify(result)


@app.route('/mongo/delete_records', methods=['POST'])
def mongo_delete_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = mongo_db.delete_records(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/mongo/truncate_table', methods=['POST'])
def mongo_truncate_table():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = mongo_db.truncate_table(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/cassandra/create_schema', methods=['POST'])
def cassandra_create_schema():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    result = cassandra_db.create_schema(host_name,user_name,password,schema_name)
    return jsonify(result)


@app.route('/cassandra/create_table', methods=['POST'])
def cassandra_create_table():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns)
    result = cassandra_db.create_table(host_name,user_name,password,schema_name,table_name,**columns)
    return jsonify(result)


@app.route('/cassandra/drop_table', methods=['POST'])
def cassandra_drop_table():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = cassandra_db.drop_table(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/cassandra/insert_record', methods=['POST'])
def cassandra_insert_record():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns)
    result = cassandra_db.insert_record(host_name,user_name,password,schema_name,table_name,**columns)
    return jsonify(result)


@app.route('/cassandra/insert_multiple_records', methods=['POST'])
def cassandra_insert_multiple_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    columns = request.values.get('columns')
    columns = json.loads(columns.replace("'",'"'))
    print(columns)
    input_file_name = request.values.get('input_file_name')
    result = cassandra_db.insert_multiple_records(host_name,user_name,password,schema_name,table_name,input_file_name,*columns)
    return jsonify(result)


@app.route('/cassandra/update_records', methods=['POST'])
def cassandra_update_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    updated_values = request.values.get('updated_values')
    updated_values = json.loads(updated_values)
    result = cassandra_db.update_records(host_name,user_name,password,schema_name,table_name,filters, **updated_values)
    return jsonify(result)


@app.route('/cassandra/retrieve_records', methods=['POST'])
def cassandra_retrieve_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = cassandra_db.retrieve_records(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/cassandra/retrieve_records_with_filter', methods=['POST'])
def cassandra_retrieve_records_with_filter():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    result = cassandra_db.retrieve_records_with_filter(host_name,user_name,password,schema_name,table_name, **filters)
    return str(result)


@app.route('/cassandra/delete_records_with_filter', methods=['POST'])
def cassandra_delete_records_with_filter():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    filters = request.values.get('filters')
    filters = json.loads(filters)
    result = cassandra_db.delete_records_with_filter(host_name,user_name,password,schema_name,table_name, **filters)
    return jsonify(result)


@app.route('/cassandra/delete_records', methods=['POST'])
def cassandra_delete_records():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = cassandra_db.delete_records(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)


@app.route('/cassandra/truncate_table', methods=['POST'])
def cassandra_truncate_table():
    host_name = request.values.get('host')
    user_name = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    table_name = request.values.get('table_name')
    result = cassandra_db.truncate_table(host_name,user_name,password,schema_name,table_name)
    return jsonify(result)



if __name__ == '__main__':
    app.run(debug=True)





