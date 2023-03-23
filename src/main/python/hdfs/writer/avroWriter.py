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


def writeAvro(inputArg, outputFile):

    if inputArg == "property":
        schemaFile = PROJECT_DIRECTORY + "/resources/property.avsc"
        dataFolder = PROJECT_DIRECTORY + "/data/idealista"


    elif inputArg == "income":
        schemaFile = PROJECT_DIRECTORY + "/resources/income.avsc"
        dataFolder = PROJECT_DIRECTORY + "/data/opendatabcn-income"


    elif inputArg == "lookup":
        schemaFile = PROJECT_DIRECTORY + "/resources/lookup.avsc"
        dataFolder = PROJECT_DIRECTORY + "/data/lookup_tables"

    if schemaFile is None:
        raise ValueError("Invalid inputArg provided")

    avroOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/avroFiles/" + outputFile \
                         # + ".avro"

    # LOAD THE SCHEMA
    schema = avro.schema.parse(open(schemaFile, "rb").read())

    # CREATE AVRO FILE
    # writer = DataFileWriter(open(avroOutputFilePath, "wb"), DatumWriter(), schema)
    # LOAD DATA
    #### EVERYTHING IN THE SAME .avro FILE
    # for filename in os.listdir(dataFolder):
    #     file = os.path.join(dataFolder, filename)
    #     print(file)
    #     if os.path.isfile(file):
    #         with open(file, 'r') as dataFile:
    #             if inputArg == "property":
    #                 data = json.load(dataFile)
    #             elif inputArg == "income" or inputArg == "lookup":
    #                 data = csv.DictReader(dataFile)
    #             for record in data:
    #                 writer.append(record)
    # writer.close()

    #### IN DIFFERENT .avro FILES
    for filename in os.listdir(dataFolder):
        file = os.path.join(dataFolder, filename)
        writer = DataFileWriter(open(avroOutputFilePath+str(filename.split('.')[0])+".avro", "wb"), DatumWriter(), schema)
        if os.path.isfile(file):
            with open(file, 'r') as dataFile:
                if inputArg == "property":
                    data = json.load(dataFile)
                elif inputArg == "income" or inputArg == "lookup":
                    data = csv.DictReader(dataFile)
                for record in data:
                    writer.append(record)
        writer.close()

if __name__ == '__main__':
    writeAvro("property", "property__")