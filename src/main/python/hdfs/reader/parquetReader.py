from pyspark.sql import SparkSession
def readParquet(fileName):
    spark = SparkSession.builder \
            .appName("Read Parquet File") \
            .getOrCreate()
    parquet_df = spark.read.parquet(fileName)
    parquet_df.show()
    spark.stop()