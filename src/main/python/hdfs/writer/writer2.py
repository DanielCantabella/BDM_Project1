import pyarrow as pa
import pyarrow.parquet as pq
import json
import os

PROJECT_DIRECTORY = "/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject"

def writeParque(inputArg, outputFile):
    avroOutputFilePath = PROJECT_DIRECTORY + "/outputFiles/parquetFiles/" + outputFile
    if inputArg == "property":
        dataFolder = PROJECT_DIRECTORY + "/data/idealista"
        json_files = [f for f in os.listdir(dataFolder) if f.endswith('.json')]
        for json_file in json_files:
            # Read the JSON file
            with open(json_file, 'r') as file:
                json_data = json.load(file)

            # Convert the JSON data to a PyArrow table
            table = pa.Table.from_pydict(json_data)

            # Write the PyArrow table to a Parquet file
            parquet_file = os.path.splitext(json_file)[0] + '.parquet'
            pq.write_table(table, parquet_file)

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
    writeParque("property", "properyjson")
    # # writeParquet("lookup", "lookupcsv")
    # # writeParquet("income", "incomecsv")
    #
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