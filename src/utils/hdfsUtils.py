from hdfs import InsecureClient
import configparser
import os
import urllib.parse
CONFIG_ROUTE = str(os.environ.get('PROJECT_DIRECTORY'))+'/src/utils/config.cfg'
def get_server_data(cfgFileDirectory):
    config = configparser.ConfigParser()
    config.read(cfgFileDirectory)
    host = config.get('data_server', 'host')
    port = config.get('data_server', 'port')
    user = config.get('data_server', 'user')
    return host, port, user


def upload_memory_to_hdfs(avro_output_file_content, hdfs_file_path: str):
    host, port, user = get_server_data(CONFIG_ROUTE)
    # Set up the HDFS client
    client = InsecureClient("http://"+host+":"+port+"/", user=user)

    if not checkIfExistsInHDFS(hdfs_file_path):
    # Upload the folder to HDFS
        with client.write(hdfs_file_path) as hdfs_file:
            hdfs_file.write(avro_output_file_content)
        print("File Uploaded in HDFS: " + str(hdfs_file_path))
    else:
        # pass
        print("File already in HDFS: " + str(hdfs_file_path))



def upload_file_to_hdfs(localFolderName: str, hdfs_file_path: str, n_threads: int = 1):
    host, port, user = get_server_data(CONFIG_ROUTE)
    # Set up the HDFS client
    client = InsecureClient("http://"+host+":"+port+"/", user=user)
    if not checkIfExistsInHDFS(hdfs_file_path):
        client.upload(hdfs_file_path, localFolderName, n_threads=n_threads)
        print("File Uploaded in HDFS: " + str(hdfs_file_path))
    else:
        print("File already in HDFS: " + str(hdfs_file_path))
        # pass

def checkIfExistsInHDFS(HDFSpath):
    host, port, user = get_server_data(CONFIG_ROUTE)
    # Set up the HDFS client
    client = InsecureClient("http://" + host + ":" + port + "/", user=user)
    if client.status(HDFSpath, strict=False):
        return True
    else:
        return False


def delete_hdfs_folder(HDFSfolder):
    host, port, user = get_server_data(CONFIG_ROUTE)
    # Set up the HDFS client
    client = InsecureClient("http://" + host + ":" + port + "/", user=user)
    if client.status(HDFSfolder, strict=False):
        client.delete(HDFSfolder, recursive=True)
        print(f"Folder {HDFSfolder} has been overwritten in HDFS")
    else:
        pass
