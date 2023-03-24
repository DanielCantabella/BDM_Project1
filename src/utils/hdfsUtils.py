from hdfs import InsecureClient

PROJECT_DIRECTORY = "/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject"
def upload_folder_to_hdfs(localFolderName: str, hdfs_folder_path: str, n_threads: int = 1):
    """
    Uploads a folder from a local machine to HDFS.

    Parameters:
    remote_uri (str): The URL of the HDFS Namenode on the remote machine.
    local_folder_path (str): The path of the folder to upload on the local machine.
    hdfs_folder_path (str): The destination path of the folder on HDFS.
    n_threads (int): The number of threads to use for parallel upload (default: 5).
    """
    # Set up the HDFS client
    client = InsecureClient("http://10.4.41.43:9870/", user='bdm')

    # Upload the folder to HDFS
    client.upload(hdfs_folder_path, localFolderName, n_threads=n_threads)
