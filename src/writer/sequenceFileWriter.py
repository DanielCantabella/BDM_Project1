from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import os
from pyspark import SparkContext, SparkConf
import csv
import json
import pyarrow as pa
import pyarrow.csv as pcsv
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import _create_row

from pyarrow.fs import HadoopFileSystem

# Directories
# PROJECT_DIRECTORY = os.environ.get('PROJECT_DIRECTORY')
PROJECT_DIRECTORY = "/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject"

def create_row_constructor(fields):
    def constructor(*values):
        return _create_row(fields, values)
    return constructor
def csv2seq(dataName,outputFolderName, outputFile=""):
    dataFolder = PROJECT_DIRECTORY + "/data/" + dataName
    if not os.path.exists(dataFolder):
        raise Exception(f"The directory {dataFolder} does not exist. Please create it.")
    sequenceFileOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/sequenceFiles/" + outputFolderName + outputFile
    os.makedirs(os.path.dirname(sequenceFileOutputFilePath), exist_ok=True)



   # HERE THE CONVERSION FROM CSV TO .seq
    conf = SparkConf().setAppName("CSV to SequenceFile")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    for filename in os.listdir(dataFolder):
        if filename.endswith('.csv'):
            # Construct the input and output file paths
            input_path = os.path.join(dataFolder, filename)
            output_path = os.path.join(sequenceFileOutputFilePath, filename.split('.')[0] + '.seq')
            csv_df = spark.read.csv(input_path, header=True, inferSchema=True)
            rdd = csv_df.rdd.map(lambda row: (None, (row.asDict(), {"metadata_key": "metadata_value"})))
            from py4j.java_gateway import JavaObject

            # create a Java Configuration object
            conf = spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration()

            # set metadata
            conf.set("metadata_key", "metadata_value")
            # save RDD as SequenceFile with Snappy compression and metadata
            rdd.saveAsSequenceFile(sequenceFileOutputFilePath, compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec", conf=conf)

            # stop SparkContext
            sc.stop()


def json2seq(dataName,outputFolderName, outputFile=""):
    dataFolder = PROJECT_DIRECTORY + "/data/" + dataName
    if not os.path.exists(dataFolder):
        raise Exception(f"The directory {dataFolder} does not exist. Please create it.")
    sequenceFileOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/sequenceFiles/" + outputFolderName + outputFile
    os.makedirs(os.path.dirname(sequenceFileOutputFilePath), exist_ok=True)

    # HERE THE CONVERSION FROM JSON TO .seq
    conf = SparkConf().setAppName("Create SequenceFile").setMaster("local")
    sc = SparkContext.getOrCreate(conf)
    json_data = sc.wholeTextFiles(dataFolder).mapValues(lambda x: json.loads(x))
    json_data.saveAsSequenceFile(sequenceFileOutputFilePath+"/outputJSON.seq")
    sc.stop()

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
    # writeSeq("property")
    writeSeq("lookup")
    writeSeq("income")
