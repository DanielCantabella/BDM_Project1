#!/bin/bash

#CONFIGURATION
#pip install -r requiements.txt
export PROJECT_DIRECTORY="/Users/danicantabella/Desktop/BDM/Labs/LandingZoneProject"

# _____________CREATE AVRO FILES______________________
# Convert opendatabcn-income CSV data to Avro files
python main.py write avro -i income
echo "opendatabcn-income CSV data converted to Avro files"
# Convert lookup_tables CSV data to Avro files
python main.py write avro -i lookup
echo "lookup_tables CSV data converted to Avro files"
# Convert idealista JSON data to Avro files
python main.py write avro -i property
echo "idealista JSON data converted to Avro files"

# _____________CREATE PARQUET FILES______________________
# Convert opendatabcn-income CSV data to Parquet files
python main.py write parquet -i income
echo "opendatabcn-income CSV data converted to Parquet files"
# Convert lookup_tables CSV data to Parquet files
python main.py write parquet -i lookup
echo "lookup_tables CSV data converted to Parquet files"
# Convert idealista JSON data to Parquet files
python main.py write parquet -i property
echo "idealista JSON data converted to Parquet files"

# _____________CREATE SEQUENCEFILE FILES_________________

