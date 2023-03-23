from hdfs import InsecureClient
import json
import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
# create HDFS client
# client = InsecureClient('http://10.4.41.43:9870', user='bdm')


# Directories
PROJECT_DIRECTORY = "/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject"


def writeParquet(inputArg, outputFile):
    avroOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/parquetFiles/" + outputFile
    if inputArg == "property":
        dataFolder = PROJECT_DIRECTORY + "/data/idealista"
        spark = SparkSession.builder \
            .appName("JSON to Parquet") \
            .getOrCreate()
        json_df = spark.read.json(dataFolder)
        # json_df.write.format("parquet").save(avroOutputFilePath)
        json_df = json_df.withColumn("input_file", input_file_name())
        json_df.write.partitionBy("input_file").parquet(avroOutputFilePath)
        spark.stop()

    elif inputArg == "income":
        dataFolder = PROJECT_DIRECTORY + "/data/opendatabcn-income"
        spark = SparkSession.builder \
            .appName("CSV to Parquet") \
            .getOrCreate()
        csv_df = spark.read.csv(dataFolder, header=True, inferSchema=True)
        csv_df.write.parquet(avroOutputFilePath)
        spark.stop()

    elif inputArg == "lookup":
        dataFolder = PROJECT_DIRECTORY + "/data/lookup_tables"
        spark = SparkSession.builder \
            .appName("CSV to Parquet") \
            .getOrCreate()
        csv_df = spark.read.csv(dataFolder, header=True, inferSchema=True)
        csv_df.write.parquet(avroOutputFilePath)
        spark.stop()


if __name__ == '__main__':
    # writeParquet("property", "properyjson")
    # writeParquet("lookup", "lookupcsv")
    # writeParquet("income", "incomecsv")

    spark = SparkSession.builder \
        .appName("Read Parquet File") \
        .getOrCreate()

    # Read the Parquet file into a PySpark DataFrame
    parquet_df = spark.read.parquet("/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject/outputFiles/parquetFiles/incomecsv/part-00000-1c4ee797-8501-4016-949a-ad026982aa19-c000.snappy.parquet")

    # Show the contents of the DataFrame
    parquet_df.show()

    # Stop the PySpark session
    spark.stop()