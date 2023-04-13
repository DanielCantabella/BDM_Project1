from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import os
from src.utils.hdfsUtils import upload_folder_to_hdfs, delete_hdfs_folder
from src.writer.avroWriter import getApiUrls, getDataFromApiUrl
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

# Directories
PROJECT_DIRECTORY = os.environ.get('PROJECT_DIRECTORY')
HDFS_DIRECTORY = os.environ.get('HDFS_DIRECTORY')

def api2parquet(data,outputFolderName, outputFile=""):
    parquetOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/parquetFiles/" + outputFolderName + outputFile
    os.makedirs(os.path.dirname(parquetOutputFilePath), exist_ok=True)
    df = pd.DataFrame(data)
    year = data[0].get("Any") #Take the year of the first trow, for example.
    filename = f"{year}_taxa_immigracio"
    pq.write_table(pa.Table.from_pandas(df), parquetOutputFilePath + filename +'.parquet')
    return parquetOutputFilePath
def csv2parquet(dataName,outputFolderName, outputFile=""):
    dataFolder = PROJECT_DIRECTORY + "/data/" + dataName
    if not os.path.exists(dataFolder):
        raise Exception(f"The directory {dataFolder} does not exist. Please create it.")
    parquetOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/parquetFiles/" + outputFolderName + outputFile
    os.makedirs(os.path.dirname(parquetOutputFilePath), exist_ok=True)

    spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()
    csv_df = spark.read.csv(dataFolder, header=True, inferSchema=True)
    csv_df = csv_df.withColumn("input_file", input_file_name())
    csv_df.write.partitionBy("input_file").parquet(parquetOutputFilePath)
    spark.stop()
    return parquetOutputFilePath

def json2parquet(dataName,outputFolderName, outputFile=""):
    dataFolder = PROJECT_DIRECTORY + "/data/" + dataName
    if not os.path.exists(dataFolder):
        raise Exception(f"The directory {dataFolder} does not exist. Please create it.")
    parquetOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/parquetFiles/" + outputFolderName + outputFile
    os.makedirs(os.path.dirname(parquetOutputFilePath), exist_ok=True)

    spark = SparkSession.builder.appName("JSON to Parquet").getOrCreate()
    json_df = spark.read.json(dataFolder)
    json_df = json_df.withColumn("input_file", input_file_name())
    json_df.write.partitionBy("input_file").parquet(parquetOutputFilePath)
    spark.stop()
    return parquetOutputFilePath

def writeParquet(inputArg):
    if inputArg == "property":
        dataName = outputFolderName = "idealista"
        parquetLocalOutputFilePath = json2parquet(dataName, outputFolderName)

    elif inputArg == "income":
        dataName = outputFolderName = "opendatabcn-income"
        parquetLocalOutputFilePath = csv2parquet(dataName, outputFolderName)

    elif inputArg == "lookup":
        dataName = outputFolderName = "lookup_tables"
        parquetLocalOutputFilePath = csv2parquet(dataName,outputFolderName)

    elif inputArg == "immigration":
        outputFolderName = "opendatabcn-immigration/"
        fileUrls=getApiUrls('https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-demo-taxa-immigracio/')
        for fileUrl in fileUrls:
            apiData = getDataFromApiUrl(fileUrl)
            if apiData is None: #2018 data seems to not have an api option
                continue
            parquetLocalOutputFilePath = api2parquet(apiData, outputFolderName)


    delete_hdfs_folder(HDFS_DIRECTORY+"parquetFiles/"+outputFolderName)  # Allows to overwrite the files in HDFS. Comment if you don't want to overwrite.
    upload_folder_to_hdfs(parquetLocalOutputFilePath, HDFS_DIRECTORY+"parquetFiles/"+outputFolderName)

if __name__ == '__main__':
    writeParquet("immigration")
    # writeParquet("property")
    # writeParquet("lookup")
    # writeParquet("income")
