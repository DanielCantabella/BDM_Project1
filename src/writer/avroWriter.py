import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import json
import os
import csv
from src.utils.hdfsUtils import upload_folder_to_hdfs
from src.utils.hdfsUtils import delete_hdfs_folder
import urllib.request
import json
import requests
from bs4 import BeautifulSoup

# Directories
PROJECT_DIRECTORY = os.environ.get('PROJECT_DIRECTORY')
HDFS_DIRECTORY = os.environ.get('HDFS_DIRECTORY')

def getApiUrls(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
    }
    # Specify the URL of the website you want to scrape
    url = 'https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/'
    response = requests.get(url, headers=headers)

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    links = []
    for link in soup.find_all('a', class_='heading'):
        links.append(link.get('href').split("/")[-1])
    return links
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


def api2avro(data, schemaName, outputFolderName, outputFile=""):
    schemaFile = PROJECT_DIRECTORY + "/resources/" + schemaName + ".avsc"
    avroOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/avroFiles/" + outputFolderName + outputFile
    if not os.path.exists(os.path.dirname(schemaFile)):
        raise Exception(f"The directory {os.path.dirname(schemaFile)} does not exist. Please create it.")
    else:
        schema = avro.schema.parse(open(schemaFile, "rb").read())

    os.makedirs(os.path.dirname(avroOutputFilePath), exist_ok=True)
    for record in data:
        year = record.get("Any")
        filename = f"{year}_taxa_immigracio"
        writer = DataFileWriter(open(avroOutputFilePath + filename + ".avro", "wb"),DatumWriter(), schema)
        for row in data:
            writer.append(row)
    writer.close()
    return avroOutputFilePath
def file2avro(inputArg, schemaName, rawDataFolderName, outputFolderName, outputFile=""):
    schemaFile = PROJECT_DIRECTORY + "/resources/" + schemaName + ".avsc"
    dataFolder = PROJECT_DIRECTORY + "/data/" + rawDataFolderName
    avroOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/avroFiles/" + outputFolderName + outputFile
    if not os.path.exists(dataFolder):
        raise Exception(f"The directory {dataFolder} does not exist. Please create it.")
    if not os.path.exists(os.path.dirname(schemaFile)):
        raise Exception(f"The directory {os.path.dirname(schemaFile)} does not exist. Please create it.")
    else:
        schema = avro.schema.parse(open(schemaFile, "rb").read())

    os.makedirs(os.path.dirname(avroOutputFilePath), exist_ok=True)

    for filename in os.listdir(dataFolder):
        file = os.path.join(dataFolder, filename)
        writer = DataFileWriter(open(avroOutputFilePath + str(filename.split('.')[0]) + ".avro", "wb"), DatumWriter(), schema)
        if os.path.isfile(file):
            with open(file, 'r') as dataFile:
                if inputArg == "property":
                    data = json.load(dataFile)
                elif inputArg == "income" or inputArg == "lookup":
                    data = csv.DictReader(dataFile)
                for record in data:
                    writer.append(record)
        writer.close()
    return avroOutputFilePath

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
        fileUrls=getApiUrls('https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/')
        for fileUrl in fileUrls:
            apiData = getDataFromApiUrl(fileUrl)
            if apiData is None: #2018 data seems to not have an api option
                continue
            avroLocalOutputFilePath = api2avro(apiData, schemaName, outputFolderName)


    if schemaName is None:
        raise ValueError("Invalid inputArg provided")

    if inputArg != "immigration":
        avroLocalOutputFilePath = file2avro(inputArg, schemaName, rawDataFolderName, outputFolderName)
    delete_hdfs_folder(HDFS_DIRECTORY+"avroFiles/"+outputFolderName) # Allows to overwrite the files in HDFS. Comment if you don't want to overwrite.
    upload_folder_to_hdfs(avroLocalOutputFilePath, HDFS_DIRECTORY+"avroFiles/"+outputFolderName)


# if __name__ == '__main__':
    # writeAvro("immigration")
    # writeAvro("property")
    # writeAvro("income")
    # writeAvro("lookup")