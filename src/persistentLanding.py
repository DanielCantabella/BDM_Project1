import configparser
from utils.functions import *


CONFIG_ROUTE = 'utils/config.cfg'

# Get the server info
config = configparser.ConfigParser()
config.read(CONFIG_ROUTE)
host = config.get('data_server', 'host')
user = config.get('data_server', 'user')
pwd = config.get('data_server', 'password')


# Execute SSH to the server
client = connect_ssh(host, user, pwd)
stdin, stdout, stderr = client.exec_command("ls")
print(stdout.read().decode())

# Exectute SFTP

# Put file
# sftp_put_file(client, remote_file_path, local_file_path)

# Get file
# sftp_get_file(client, remote_path, local_path):

# Close SHH
close_ssh(client)




