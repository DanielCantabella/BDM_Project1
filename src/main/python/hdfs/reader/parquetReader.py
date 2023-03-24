from pyspark.sql import SparkSession
import pyarrow.parquet as pq
import pyarrow
import pandas as pd
def readParquet(fileName):
    spark = SparkSession.builder.appName("Read Parquet File").getOrCreate()
    parquet_df = spark.read.parquet(fileName, )
    parquet_df.show()
    spark.stop()


if __name__ == '__main__':
    readParquet("/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject/outputFiles/parquetFiles/idealista/input_file=file%3A%2F%2F%2FUsers%2Fdanicantabella%2FDesktop%2FBDM%2FLabs%2FLandingZoneProject%2Fdata%2Fidealista%2F2020_01_02_idealista.json/part-00003-b56e45b9-8a21-49af-af09-3a17c7b44841.c000.snappy.parquet")
    # import pyarrow.parquet as pq
    #
    # # Create Parquet file reader object
    # parquet_file = pq.ParquetFile('/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject/outputFiles/parquetFiles/idealista/input_file=file%3A%2F%2F%2FUsers%2Fdanicantabella%2FDesktop%2FBDM%2FLabs%2FLandingZoneProject%2Fdata%2Fidealista%2F2020_01_02_idealista.json/part-00003-b56e45b9-8a21-49af-af09-3a17c7b44841.c000.snappy.parquet')
    #
    # # Get metadata and print it
    # metadata = parquet_file.metadata
    # print(metadata.metadata)