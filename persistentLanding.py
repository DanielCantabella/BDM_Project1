import configparser
from datetime import datetime
from hdfs import InsecureClient
import avro.schema
import avro.io
import io
from avro.datafile import DataFileReader
from avro.io import DatumReader
import happybase
import json

CONFIG_ROUTE = 'src/utils/config.cfg'

def create_table_if_not_exists(connection, table):
    if table.encode() not in connection.tables():
        connection.create_table(table, {'data': dict(), 'metadata': dict()})

def read_avro_and_insert_hbase(client, table, path):
    # generate key
    info_list = path.split('.')[0].split("/")[-1].split("%")
    source = info_list[0]
    ext = info_list[1]
    file_name = info_list[2]
    if len(info_list) < 4:
        #For the API data
        now = datetime.now()
        vtime = now.strftime("%Y-%m-%dT%H-%M-%S")
    else:
        vtime = f"{info_list[3]}T{info_list[4]}"
    row_key = '$'.join([source, ext, file_name, vtime])
    # get value
    row_value = dict()
    data = list()
    with client.read(path) as reader:
        contents = reader.read()
        bytes_reader = io.BytesIO(contents)
        datafile_reader = avro.datafile.DataFileReader(bytes_reader, avro.io.DatumReader())
        schema = json.loads(datafile_reader.schema)
        # add file as data
        for record in datafile_reader:
            data.append(record)
        row_value['data:file'] = bytes(json.dumps(data), encoding='utf-8')
        # add schema as metadata
        row_value['metadata:schema'] = bytes(json.dumps(schema['fields']), encoding='utf-8')
        # insert column
        table.put(row_key, row_value)
        datafile_reader.close()

def read_raw_and_insert_hbase(client, table, path):
    # generate key
    info = path.split("/")
    info_list = info[-1].split('%')
    source = info[-2]
    if 'rawFiles' in source:
        return
    full_fn = info_list[-1].split('.')
    file_name = full_fn[0]
    ext = full_fn[1]
    row_key = '$'.join([source, ext, file_name])
    with client.read(path) as f:
        file_contents = f.read()
        # get value
        row_value = dict()
        row_value['data:file'] = file_contents
        table.put(row_key, row_value)

def read_hdfs_and_insert_hbase(client,table, path):
    file_status = client.status(path)
    if file_status['type'] == 'DIRECTORY':
        for file in client.list(path):
            read_hdfs_and_insert_hbase(client,table, '/'.join([path, file]))
    else:
        print(f"Inserting: {path}")
        try:
            file_ext = path.split(".")[-1]
        except:
            return
        # Avro files
        if file_ext == 'avro':
            read_avro_and_insert_hbase(client, table, path)
        # Parquet files
        elif file_ext == 'parquet':
            return
        # Raw data
        else:
            read_raw_and_insert_hbase(client, table, path)

def print_hbase_table(connection, tablename):
    table = connection.table(tablename)
    for key, data in table.scan():
        print(f"Row key: {key}")
        for column, value in data.items():
            print(f"    Column: {column} => Value: {value}")

def delete_hbase_table(connection, tablename):
    connection.delete_table(tablename, disable=True)
    print(f"Left tables: {connection.tables()}")

if __name__ == '__main__':
    # Get the server info
    config = configparser.ConfigParser()
    config.read(CONFIG_ROUTE)
    host = config.get('data_server', 'host')
    user = config.get('data_server', 'user')
    tablename = config.get('hbase', 'tablename')

    # Get the routes
    hdfs_path = config.get('routes', 'hdfs')
    host_hdfs = 'http://' + host + ':9870'
    # Connect to hdfs
    client = InsecureClient(host_hdfs, user=user)
    files = client.list(hdfs_path)
    # Connect to HBase
    connection = happybase.Connection(host=host, port=9090)
    connection.open()

    # Load tables from temporal to persistent landing
    create_table_if_not_exists(connection, tablename)
    table = connection.table(tablename)
    read_hdfs_and_insert_hbase(client, table, hdfs_path)

    # # Print table content
    # print_hbase_table(connection, tablename)

    # # Delete table
    # delete_hbase_table(connection, tablename)

    # Close connection to HBase
    connection.close()

