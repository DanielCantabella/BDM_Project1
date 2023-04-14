from hdfs import InsecureClient
import configparser
import os
CONFIG_ROUTE = str(os.environ.get('PROJECT_DIRECTORY'))+'/src/utils/config.cfg'
def get_server_data(cfgFileDirectory):
    config = configparser.ConfigParser()
    config.read(cfgFileDirectory)
    host = config.get('data_server', 'host')
    port = config.get('data_server', 'port')
    user = config.get('data_server', 'user')
    return host, port, user

def upload_memory_to_hdfs(avro_output_file_content, hdfs_file_path: str):
    """
    Uploads a folder from a local machine to HDFS.

    Parameters:
    remote_uri (str): The URL of the HDFS Namenode on the remote machine.
    local_folder_path (str): The path of the folder to upload on the local machine.
    hdfs_folder_path (str): The destination path of the folder on HDFS.
    n_threads (int): The number of threads to use for parallel upload (default: 5).
    """
    host, port, user = get_server_data(CONFIG_ROUTE)
    # Set up the HDFS client
    client = InsecureClient("http://"+host+":"+port+"/", user=user)

    # hdfs_dir_path = "/".join(hdfs_file_path.split("/")[:-1])+"/"
    # print("EIII" + hdfs_dir_path)
    # # Create the directory in HDFS if it does not exist
    # if not client.status(hdfs_dir_path, strict=False):
    #     client.makedirs(hdfs_dir_path)

    # Upload the folder to HDFS
    with client.write(hdfs_file_path) as hdfs_file:
        hdfs_file.write(avro_output_file_content)

def upload_folder_to_hdfs(localFolderName: str, hdfs_folder_path: str, n_threads: int = 1):
    """
    Uploads a folder from a local machine to HDFS.

    Parameters:
    remote_uri (str): The URL of the HDFS Namenode on the remote machine.
    local_folder_path (str): The path of the folder to upload on the local machine.
    hdfs_folder_path (str): The destination path of the folder on HDFS.
    n_threads (int): The number of threads to use for parallel upload (default: 5).
    """
    host, port, user = get_server_data(CONFIG_ROUTE)
    # Set up the HDFS client
    client = InsecureClient("http://"+host+":"+port+"/", user=user)
    # Upload the folder to HDFS
    client.upload(hdfs_folder_path, localFolderName, n_threads=n_threads)


def delete_hdfs_folder(HDFSfolder):
    host, port, user = get_server_data(CONFIG_ROUTE)
    # Set up the HDFS client
    client = InsecureClient("http://" + host + ":" + port + "/", user=user)
    if client.status(HDFSfolder, strict=False):
        client.delete(HDFSfolder, recursive=True)
        print(f"Folder {HDFSfolder} has been overwritten in HDFS")
    else:
        pass
