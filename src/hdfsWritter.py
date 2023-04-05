import pyarrow.hdfs as hdfs
# NAMENODE_HOST = "hdfs://butterfree.fib.upc.es"
NAMENODE_HOST = "hdfs://butterfree.fib.upc.es:27000"
# USER = "masterBD215"
USER = "bdm"
# PASSWORD = "Vj7DUKGc2s"
PASSWORD = "bdm"

# client = hdfs.connect(host='butterfree.fib.upc.es', port=27000)
client = hdfs.connect(host=NAMENODE_HOST, user = USER)
# dirs = client.ls('/')
# for dir in dirs:
#     print(dir['name'])
#
# with open('/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject/src/writer/removeLater.txt', 'rb') as f:
#     data = f.read()
# #
# with client.open('/user/bdm/removeLater.txt', 'wb') as f:
#     f.write(data)
