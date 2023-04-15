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
def iterate_directory(directory):
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)  # Get the full path of the item

        if os.path.isfile(item_path):
            modification_time = os.path.getmtime(item_path)
            modification_time_datetime = datetime.datetime.fromtimestamp(modification_time).strftime("%Y-%m-%d_%H-%M-%S")
            date, time = modification_time_datetime.split('_')
            rawDataFolderName = item_path.split("/")[-2]
            dataType = item_path.split(".")[-1]
            outputHDFSfolderName = HDFS_DIRECTORY + "rawFiles/" + rawDataFolderName + "%" + dataType + "%" + item.split(".")[0] + "%" + date + "%" + time + "." + dataType
            upload_memory_to_hdfs(item_path, outputHDFSfolderName)

        # Check if the item is a directory
        elif os.path.isdir(item_path):
            iterate_directory(item_path)
        # Handle other types of items (e.g., symbolic links, etc.)
        else:
            print(f'{item} is not a file or directory')

def writeRaw(inputArg):

    if inputArg == "property":
        dataFolder = "idealista"
    elif inputArg == "income":
        dataFolder = "opendatabcn-income"
    elif inputArg == "lookup":
        dataFolder = "lookup_tables"

    elif inputArg == "immigration":
        outputFolderName = "opendatabcn-immigration"
        fileUrls , filenames = getApiUrls('https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/')

        for index, fileUrl in enumerate(fileUrls):
            apiData = getDataFromApiUrl(fileUrl)
            if apiData is None: #2018 data seems to not have an api option
                continue
            bytes_data = pickle.dumps(apiData)
            outputHDFSfolderName = HDFS_DIRECTORY+"rawFiles/" + outputFolderName + "%" + "json%" + filenames[index]+ ".json"
            upload_memory_to_hdfs(bytes_data, outputHDFSfolderName)


    if inputArg != "immigration":
        dataDirectory = PROJECT_DIRECTORY + "/data/" + dataFolder
        iterate_directory(dataDirectory)




if __name__ == '__main__':
    # writeRaw("immigration")
    # writeRaw("property")
    writeRaw("income")
    writeRaw("lookup")
    # iterate_directory(PROJECT_DIRECTORY + "/data/")


