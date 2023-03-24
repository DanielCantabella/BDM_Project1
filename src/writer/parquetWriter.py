from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name



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
    writeParquet("lookup")
    writeParquet("income")
