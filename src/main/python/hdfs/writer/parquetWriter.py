from hdfs import InsecureClient
import json
import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
# create HDFS client
# client = InsecureClient('http://10.4.41.43:9870', user='bdm')


# Directories
PROJECT_DIRECTORY = "/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject"


def csv2parquet(dataName,outputFolderName, outputFile=""):
    dataFolder = PROJECT_DIRECTORY + "/data/" + dataName
    parquetOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/parquetFiles/" + outputFolderName + outputFile
    spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()
    csv_df = spark.read.csv(dataFolder, header=True, inferSchema=True)
    csv_df = csv_df.withColumn("input_file", input_file_name())
    csv_df.write.partitionBy("input_file").parquet(parquetOutputFilePath)
    spark.stop()

def json2parquet(dataName,outputFolderName, outputFile=""):
    dataFolder = PROJECT_DIRECTORY + "/data/" + dataName
    parquetOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/parquetFiles/" + outputFolderName + outputFile
    spark = SparkSession.builder.appName("JSON to Parquet").getOrCreate()
    json_df = spark.read.json(dataFolder)
    json_df = json_df.withColumn("input_file", input_file_name())
    json_df.write.partitionBy("input_file").parquet(parquetOutputFilePath)
    spark.stop()

def writeParquet(inputArg):
    if inputArg == "property":
        dataName = outputFolderName = "idealista"
        json2parquet(dataName, outputFolderName)

    elif inputArg == "income":
        dataName = outputFolderName = "opendatabcn-income"
        csv2parquet(dataName, outputFolderName)

    elif inputArg == "lookup":
        dataName = outputFolderName = "lookup_tables"
        csv2parquet(dataName,outputFolderName)

if __name__ == '__main__':
    writeParquet("property")
    # writeParquet("lookup")
    # writeParquet("income")

    # spark = SparkSession.builder \
    #     .appName("Read Parquet File") \
    #     .getOrCreate()
    #
    # # Read the Parquet file into a PySpark DataFrame
    # parquet_df = spark.read.parquet("/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject/outputFiles/parquetFiles/incomecsv/part-00000-1c4ee797-8501-4016-949a-ad026982aa19-c000.snappy.parquet")
    #
    # # Show the contents of the DataFrame
    # parquet_df.show()
    #
    # # Stop the PySpark session
    # spark.stop()