from hdfs import InsecureClient
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import os
import csv

# create HDFS client
# client = InsecureClient('http://10.4.41.43:9870', user='bdm')


# Directories
PROJECT_DIRECTORY = "/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject"


def file2avro(inputArg, schemaName, rawDataFolderName, outputFolderName, outputFile=""):
    schemaFile = PROJECT_DIRECTORY + "/resources/" + schemaName + ".avsc"
    dataFolder = PROJECT_DIRECTORY + "/data/" + rawDataFolderName
    avroOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/avroFiles/" + outputFolderName + outputFile
    schema = avro.schema.parse(open(schemaFile, "rb").read())

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

    if schemaName is None:
        raise ValueError("Invalid inputArg provided")

    file2avro(inputArg, schemaName, rawDataFolderName, outputFolderName)


if __name__ == '__main__':
    writeAvro("property")