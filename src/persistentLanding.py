import configparser
from utils.functions import *
from hdfs import InsecureClient
import avro.schema
import avro.io
import io
from avro.datafile import DataFileReader
from avro.io import DatumReader
import happybase
import json

CONFIG_ROUTE = 'utils/config.cfg'


# Get the server info
config = configparser.ConfigParser()
config.read(CONFIG_ROUTE)
host = config.get('data_server', 'host')
user = config.get('data_server', 'user')
tablename = config.get('hbase', 'tablename')

# Get the routes
hdfs_path = config.get('routes', 'hdfs')

host_hdfs = 'http://'+ host +':9870'

def create_table_if_not_exists(table):
    if table.encode() not in connection.tables():
        connection.create_table(table, {'data': dict(), 'metadata': dict()})

# Connect to hdfs
client = InsecureClient(host_hdfs, user=user)
files = client.list(hdfs_path)

# Connect to HBase
connection = happybase.Connection(host=host, port=9090)
connection.open()
create_table_if_not_exists(tablename)
table = connection.table(tablename)

for folder in files:
    path = hdfs_path + folder
    for file in client.list(path):
        filename = '/'.join([path, file])
        with client.read(filename) as reader:
            contents = reader.read()
            bytes_reader = io.BytesIO(contents)
            datafile_reader = avro.datafile.DataFileReader(bytes_reader, avro.io.DatumReader())
            schema = json.loads(datafile_reader.schema)

            # generate key
            source = folder
            file_ext = schema['name'].split('.')[1] if '.' in schema['name'] else 'no_extension'
            file_name = file.split('.')[0] if '.' in file else file
            row_key = '$'.join([source, file_ext, file_name])
            row_value = dict()
            data = list()
            # add file as data
            for record in datafile_reader:
                data.append(record)
            row_value['data:file'] = bytes(json.dumps(data), encoding='utf-8')
            # add schema as metadata
            row_value['metadata:schema'] = bytes(json.dumps(schema['fields']), encoding='utf-8')
            # insert column
            table.put(row_key, row_value)
            datafile_reader.close()
        # break
    # break


print(connection.tables())

# table = connection.table('datalake')
#
# for key, data in table.scan():
#     print(f"Row key: {key}")
#     for column, value in data.items():
#         print(f"    Column: {column} => Value: {value}")

# connection.delete_table('datalake', disable=True)
# print(connection.tables())


# print(table.families())
#
# # Close connection to HBase
connection.close()

