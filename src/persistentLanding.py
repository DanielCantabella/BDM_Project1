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
# HDFS_PATH = '/home/bdm/BDM_Software/hadoop/bin'
HDFS_PATH = '/user/bdm/Temporal_LZ/avroFiles/'

# Get the server info
config = configparser.ConfigParser()
config.read(CONFIG_ROUTE)
host = config.get('data_server', 'host')
user = config.get('data_server', 'user')
pwd = config.get('data_server', 'password')

host_hdfs = 'http://'+ host +':9870'

def create_table_if_not_exists(table):
    if table.encode() not in connection.tables():
        connection.create_table(table, {'cf': {}})
        print('tabla creada')

# Connect to hdfs
client = InsecureClient(host_hdfs, user=user)
files = client.list(HDFS_PATH)

# Connect to HBase
connection = happybase.Connection(host=host, port=9090)
connection.open()

for folder in files:
    path = HDFS_PATH + folder
    # the folder is equivalent to the table name
    create_table_if_not_exists(folder)
    table = connection.table(folder)
    for file in client.list(path):
        filename = '/'.join([path, file])
        print(filename)
        with client.read(filename) as reader:
            contents = reader.read()
            bytes_reader = io.BytesIO(contents)
            datafile_reader = avro.datafile.DataFileReader(bytes_reader, avro.io.DatumReader())
            schema = json.loads(datafile_reader.schema)
            print(schema)
            for i, record in enumerate(datafile_reader):
                # print(record)
                row_key = (file.split('.')[0]+'_'+str(i+1)).encode()  # use id field as row key
                data = {}
                for field in schema['fields']:
                    data['cf:'+field['name']] = str(record[field['name']]).encode()  # convert all fields to bytes
                table.put(row_key, data)
                # print(data)
            datafile_reader.close()
        break
    break


# print(connection.tables())

table = connection.table('idealista')

for key, data in table.scan():
    print(f"Row key: {key}")
    for column, value in data.items():
        print(f"    Column: {column} => Value: {value}")

# print(table.families())
#
# # Close connection to HBase
connection.close()


# table = connection.table('table_test')

# scan table and print values
# for key, data in table.scan():
#     print(f"Row key: {key}")
#     for column, value in data.items():
#         print(f"    Column: {column} => Value: {value}")
#
# # Check if table exists
# table_name = 'table_test'
#
#
# table = connection.table(table_name)





# # Execute SSH to the server
# client = connect_ssh(host, user, pwd)
# stdin, stdout, stderr = client.exec_command("cd BDM_Software/hadoop/bin && ./hdfs dfs -ls")
# print(stdout.read().decode())
#
#
# # Exectute SFTP
#
# # Put file
# # sftp_put_file(client, remote_file_path, local_file_path)
#
# # Get file
# remote_path = 'BDM_Software/hadoop/bin'
# local_path = 'temp/aux_1.avro'
# # sftp_get_file(client, remote_path, local_path):
#
# # Close SHH
# close_ssh(client)




