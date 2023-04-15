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
# Directories
PROJECT_DIRECTORY = os.environ.get('PROJECT_DIRECTORY')
HDFS_DIRECTORY = os.environ.get('HDFS_DIRECTORY')

def getApiUrls(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
    }
    # Specify the URL of the website you want to scrape
    # url = 'https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/'
    response = requests.get(url, headers=headers)
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    links = []
    for link in soup.find_all('a', class_='heading'):
        links.append([link.get('href').split("/")[-1],link.get('title').split(".")[0]])
    urlsRefs = [sublist[0] for sublist in links]
    filenames = [sublist[1] for sublist in links]
    return urlsRefs, filenames
def getDataFromApiUrl(fileLink):
    url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search_sql?sql=SELECT%20*%20from%20%22' + fileLink + '%22'
    try:
        response = urllib.request.urlopen(url)
        content = response.read()
        content_str = content.decode('utf-8')
        json_data = json.loads(content_str)

        # Access the records from the JSON object
        records = json_data['result']['records']
        return records
    except:
        print("No API for URL:" + url)


def api2avro(data, schemaName):
    schemaFile = PROJECT_DIRECTORY + "/resources/" + schemaName + ".avsc"
    if not os.path.exists(os.path.dirname(schemaFile)):
        raise Exception(f"The directory {os.path.dirname(schemaFile)} does not exist. Please create it.")
    else:
        schema = avro.schema.parse(open(schemaFile, "rb").read())
    avro_output_file = io.BytesIO() # Create an in-memory file object
    datum_writer = avro.io.DatumWriter(schema) # Create an Avro DatumWriter
    avro_encoder = avro.io.BinaryEncoder(avro_output_file) # Create an Avro BinaryEncoder
    # Parse JSON string
    for item in data:
        datum_writer.write(item, avro_encoder)
    # Get the contents of the in-memory file object
    avro_output_file_content = avro_output_file.getvalue()
    # Return the Avro content as bytes
    return avro_output_file_content

def file2avro(inputArg, schemaName, rawDataFolderName, outputFolderName):
    schemaFile = PROJECT_DIRECTORY + "/resources/" + schemaName + ".avsc"
    dataFolder = PROJECT_DIRECTORY + "/data/" + rawDataFolderName

    if not os.path.exists(dataFolder):
        raise Exception(f"The directory {dataFolder} does not exist. Please create it.")
    if not os.path.exists(os.path.dirname(schemaFile)):
        raise Exception(f"The directory {os.path.dirname(schemaFile)} does not exist. Please create it.")
    else:
        schema = avro.schema.parse(open(schemaFile, "rb").read())

    for filename in os.listdir(dataFolder):
        file = os.path.join(dataFolder, filename)
        modification_time = os.path.getmtime(file)
        modification_time_datetime = datetime.datetime.fromtimestamp(modification_time).strftime("%Y-%m-%d_%H-%M-%S")
        date, time = modification_time_datetime.split('_')
        if os.path.isfile(file):
            with open(file, 'r') as dataFile:
                avro_output_file = io.BytesIO() # Create an in-memory file object
                datum_writer = avro.io.DatumWriter(schema) # Create an Avro DatumWriter
                avro_encoder = avro.io.BinaryEncoder(avro_output_file) # Create an Avro BinaryEncoder

                dataType = file.split(".")[-1]
                if dataType == "json":
                    data = json.load(dataFile)
                elif dataType == "csv":
                    data = csv.DictReader(dataFile)
                else:
                    data = dataFile

                for item in data:
                    datum_writer.write(item, avro_encoder)
                avro_output_file_content = avro_output_file.getvalue()

                outputHDFSfolderName = HDFS_DIRECTORY + "avroFiles/"  + rawDataFolderName[:-1] + "%" + dataType + "%" + filename.split(".")[0] + "%" + date + "%" + time + ".avro"
                # outputHDFSfolderName = HDFS_DIRECTORY + "avroFiles/" + outputFolderName + "opendatabcn-income$" + "csv$" + filename.split(".")[0] + modification_time_datetime + ".avro"
                upload_memory_to_hdfs(avro_output_file_content, outputHDFSfolderName)



def writeAvro(inputArg):

    if inputArg == "property":
        schemaName = "property"
        rawDataFolderName = outputFolderName = "idealista/"

    elif inputArg == "income":
        schemaName = "income"
        rawDataFolderName = outputFolderName = "opendatabcn-income/"

    elif inputArg == "lookup":
        schemaName = "lookup"
        rawDataFolderName = outputFolderName = "lookup_tables/"

    elif inputArg == "immigration":
        schemaName = "immigration"
        outputFolderName = "opendatabcn-immigration/"
        fileUrls , filenames = getApiUrls('https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/')

        for index, fileUrl in enumerate(fileUrls):
            apiData = getDataFromApiUrl(fileUrl)
            if apiData is None: #2018 data seems to not have an api option
                continue
            memoryFile = api2avro(apiData, schemaName)
            outputHDFSfolderName = HDFS_DIRECTORY+"avroFiles/" + outputFolderName[:-1] + "%" + "json%" + filenames[index]+ ".avro"
            # outputHDFSfolderName = HDFS_DIRECTORY+"avroFiles/" + outputFolderName + "opendatabcn-immigration$" + "json$" + filenames[index]+ ".avro"
            upload_memory_to_hdfs(memoryFile, outputHDFSfolderName)


    if schemaName is None:
        raise ValueError("Invalid inputArg provided")

    if inputArg != "immigration":
        file2avro(inputArg, schemaName, rawDataFolderName, outputFolderName)




if __name__ == '__main__':
    writeAvro("immigration")
    writeAvro("property")
    writeAvro("income")
    writeAvro("lookup")

