#!/bin/bash

#CONFIGURATION
#pip install -r requiements.txt
export PROJECT_DIRECTORY="/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject"
export HDFS_DIRECTORY="/user/bdm/Temporal_LZ/"

# _____________CREATE AND UPLOAD AVRO FILES______________________
echo "CREATING AND LOADING AVRO FILES IN HDFS..."
# Convert opendatabcn-income CSV data to Avro files
python main.py write avro -i income
echo "opendatabcn-income .avro files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/avroFiles/opendatabcn-income/"
echo "opendatabcn-income .avro files uploaded in HDFS path: ${HDFS_DIRECTORY}avroFiles/opendatabcn-income/"
# Convert lookup_tables CSV data to Avro files
python main.py write avro -i lookup
echo "lookup_tables .avro files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/avroFiles/lookup_tables/"
echo "lookup_tables .avro files uploaded in HDFS path: ${HDFS_DIRECTORY}avroFiles/lookup_tables/"
# Convert idealista JSON data to Avro files
python main.py write avro -i property
echo "idealista .avro files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/avroFiles/idealista/"
echo "idealista .avro files uploaded in HDFS path: ${HDFS_DIRECTORY}avroFiles/idealista/"
# Convert opendatabcn-immigration API data to Avro files
python main.py write avro -i immigration
echo "opendatabcn-immigration .avro files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/avroFiles/opendatabcn-immigration/"
echo "opendatabcn-immigration .avro files uploaded in HDFS path: ${HDFS_DIRECTORY}avroFiles/opendatabcn-immigration/"

# _____________CREATE AND UPLOAD PARQUET FILES______________________
echo "CREATING AND LOADING PARQUET FILES IN HDFS..."
# Convert opendatabcn-income CSV data to Parquet files
python main.py write parquet -i income
echo "opendatabcn-income .parquet files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/parquetFiles/opendatabcn-income/"
echo "opendatabcn-income .parquet files uploaded in HDFS path: ${HDFS_DIRECTORY}parquetFiles/opendatabcn-income/"
# Convert lookup_tables CSV data to Parquet files
python main.py write parquet -i lookup
echo "lookup_tables .parquet files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/parquetFiles/lookup_tables/"
echo "lookup_tables .parquet files uploaded in HDFS path: ${HDFS_DIRECTORY}parquetFiles/lookup_tables/"
# Convert idealista JSON data to Parquet files
python main.py write parquet -i property
echo "idealista .parquet files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/parquetFiles/idealista/"
echo "idealista .parquet files uploaded in HDFS path: ${HDFS_DIRECTORY}parquetFiles/idealista/"
# Convert opendatabcn-immigration API data to Parquet files
python main.py write parquet -i property
echo "opendatabcn-immigration .parquet files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/parquetFiles/opendatabcn-immigration/"
echo "opendatabcn-immigration .parquet files uploaded in HDFS path: ${HDFS_DIRECTORY}parquetFiles/opendatabcn-immigration/"

# _____________CREATE AND UPLOAD SEQUENCEFILE FILES_________________
#echo "CREATING AND LOADING SEQUENCE FILES IN HDFS..."
## Convert opendatabcn-income CSV data to Sequence files
#python main.py write sequence -i income
#echo "opendatabcn-income .seq files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/sequenceFiles/opendatabcn-income/"
#echo "opendatabcn-income .seq files uploaded in HDFS path: ${HDFS_DIRECTORY}sequenceFiles/opendatabcn-income/"
## Convert lookup_tables CSV data to Sequence files
#python main.py write sequence -i lookup
#echo "lookup_tables .seq files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/sequenceFiles/lookup_tables/"
#echo "lookup_tables .seq files uploaded in HDFS path: ${HDFS_DIRECTORY}sequenceFiles/lookup_tables/"
## Convert idealista JSON data to Sequence files
#python main.py write sequence -i property
#echo "idealista .seq files saved locally in path: ${PROJECT_DIRECTORY}/outputFiles/sequenceFiles/idealista/"
#echo "idealista .seq files uploaded in HDFS path: ${HDFS_DIRECTORY}sequenceFiles/idealista/"

