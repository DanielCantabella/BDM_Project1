from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import os


# Directories
PROJECT_DIRECTORY = os.environ.get('PROJECT_DIRECTORY')

def csv2seq(dataName,outputFolderName, outputFile=""):
    dataFolder = PROJECT_DIRECTORY + "/data/" + dataName
    if not os.path.exists(dataFolder):
        raise Exception(f"The directory {dataFolder} does not exist. Please create it.")
    sequenceFileOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/sequenceFiles/" + outputFolderName + outputFile
    os.makedirs(os.path.dirname(sequenceFileOutputFilePath), exist_ok=True)

   # HERE THE CONVERSION FROM CSV TO .seq

def json2seq(dataName,outputFolderName, outputFile=""):
    dataFolder = PROJECT_DIRECTORY + "/data/" + dataName
    if not os.path.exists(dataFolder):
        raise Exception(f"The directory {dataFolder} does not exist. Please create it.")
    sequenceFileOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/sequenceFiles/" + outputFolderName + outputFile
    os.makedirs(os.path.dirname(sequenceFileOutputFilePath), exist_ok=True)

    # HERE THE CONVERSION FROM JSON TO .seq

def writeSeq(inputArg):
    if inputArg == "property":
        dataName = outputFolderName = "idealista"
        json2seq(dataName, outputFolderName)

    elif inputArg == "income":
        dataName = outputFolderName = "opendatabcn-income"
        csv2seq(dataName, outputFolderName)

    elif inputArg == "lookup":
        dataName = outputFolderName = "lookup_tables"
        csv2seq(dataName,outputFolderName)

if __name__ == '__main__':
    writeSeq("property")
    writeSeq("lookup")
    writeSeq("income")
