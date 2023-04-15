#!/bin/bash

#CONFIGURATION
#pip install -r requiements.txt

export PROJECT_DIRECTORY="$PWD"
export HDFS_DIRECTORY="/user/bdm/Temporal_LZ/"

# _____________CREATE AND UPLOAD AVRO FILES______________________
echo "CREATING AND LOADING AVRO FILES IN HDFS..."
python main.py write avro
echo "All files from ${PROJECT_DIRECTORY}/data have been uploaded to HDFS path: ${HDFS_DIRECTORY}"
echo "Files that could not be uploaded as Avro files have been uploaded in their original format"
#
# _____________UPLOAD RAW FILES TO HDFS______________________
echo "UPLOADING RAW FILES IN HDFS..."
python main.py write raw
echo "Raw data files from ${PROJECT_DIRECTORY}/data uploaded in HDFS path: ${HDFS_DIRECTORY}"


## _____________CREATE AND UPLOAD PARQUET FILES______________________
#echo "CREATING AND LOADING PARQUET FILES IN HDFS..."
#python main.py write parquet
#echo "All files from ${PROJECT_DIRECTORY}/data have been uploaded to HDFS path: ${HDFS_DIRECTORY}"
#echo "Files that could not be uploaded as Parquet files have been uploaded in their original format"
##


