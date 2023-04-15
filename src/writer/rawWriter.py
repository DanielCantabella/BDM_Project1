import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import json
import os
import csv
from src.utils.hdfsUtils import upload_file_to_hdfs
from src.utils.hdfsUtils import delete_hdfs_folder
import urllib.request
import json
import requests
from bs4 import BeautifulSoup
import io
from src.utils.hdfsUtils import upload_memory_to_hdfs
import time
import datetime
from src.writer.avroWriter import getApiUrls, getDataFromApiUrl, api2avro
import pickle

# Directories
PROJECT_DIRECTORY = os.environ.get('PROJECT_DIRECTORY')
HDFS_DIRECTORY = os.environ.get('HDFS_DIRECTORY')

def get_raw_data(data):
    output_file = io.BytesIO()  # Create an in-memory file object
    # Parse JSON string
    for item in data:
        output_file.write(bytes(str(item), 'utf-8'))
    output_file.seek(0)
    # Get the contents of the in-memory file object
    output_file_content = output_file.getvalue()
    # Return the raw data as bytes
    return output_file_content
def iterate_directory(directory):
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)  # Get the full path of the item

        if os.path.isfile(item_path):
            modification_time = os.path.getmtime(item_path)
            modification_time_datetime = datetime.datetime.fromtimestamp(modification_time).strftime("%Y-%m-%d_%H-%M-%S")
            date, time = modification_time_datetime.split('_')
            source = item_path.split("/")[-2]
            dataType = item_path.split(".")[-1]
            outputHDFSfolderName = HDFS_DIRECTORY + source + "%" + item.split(".")[0] + "%" + date + "%" + time + "." + dataType
            # outputHDFSfolderName = HDFS_DIRECTORY + "rawFiles/" + rawDataFolderName + "%" + dataType + "%" + item.split(".")[0] + "%" + date + "%" + time + "." + dataType
            upload_file_to_hdfs(item_path, outputHDFSfolderName)

        # Check if the item is a directory
        elif os.path.isdir(item_path):
            iterate_directory(item_path)
        # Handle other types of items (e.g., symbolic links, etc.)
        else:
            print(f'{item} is not a file or directory')

def writeRaw(source):

    if source == "opendatabcn-immigration":
        fileUrls , filenames = getApiUrls('https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/')

        for index, fileUrl in enumerate(fileUrls):
            apiData = getDataFromApiUrl(fileUrl)
            if apiData is None: #2018 data seems to not have an api option
                continue
            bytes_data = get_raw_data(apiData)
            outputHDFSfolderName = HDFS_DIRECTORY + source + "%" + filenames[index]+ ".json"
            # outputHDFSfolderName = HDFS_DIRECTORY+"rawFiles/" + source + "%" + "json%" + filenames[index]+ ".json"
            upload_memory_to_hdfs(bytes_data, outputHDFSfolderName)

    else:
        dataDirectory = os.path.join(PROJECT_DIRECTORY, "data", source)
        iterate_directory(dataDirectory)




# if __name__ == '__main__':
    # writeRaw("opendatabcn-immigration")
    # writeRaw("idealista")
    # writeRaw("opendatabcn-income")
    # writeRaw("lookup_tables")
    # writeRaw("images")
    # writeRaw("")



