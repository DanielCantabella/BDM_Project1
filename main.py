from src.writer.avroWriter import writeAvro
from src.writer.parquetWriter import writeParquet
from src.writer.sequenceFileWriter import writeSeq
from src.reader.avroReader import readAvro
from src.reader.parquetReader import readParquet
from src.reader.sequenceReader import readSeq
from src.writer.rawWriter import writeRaw
import os
import argparse


parser = argparse.ArgumentParser()
# writeGroup = parser.add_mutually_exclusive_group()
parser.add_argument('mode', choices=['write', 'read'], help='Choose write or read mode')
parser.add_argument('format', choices=['avro', 'parquet', 'raw'], help='Choose avro, parquet or raw formats')
parser.add_argument('-i', '--source', help='Input file options: {opendatabcn-income, idealista, lookup_tables, opendatabcn-immigration}')
parser.add_argument('-r', '--readFile', help='Reading file path.')

args = parser.parse_args()

def ifNotAvroThenLoadRaw(source):
    try:
        writeAvro(source)
    except Exception as e:
        print(f"Failed to write Avro: {e}. Falling back to raw write.")
        writeRaw(source)

def ifNotParquetThenLoadRaw(source):
    try:
        writeParquet(source)
    except Exception as e:
        print(f"Failed to write Parquet: {e}. Falling back to raw write.")
        writeRaw(source)

if __name__ == '__main__':
    dataFolders= os.listdir("./data")
    apiOption = "opendatabcn-immigration"
    if args.format == 'avro' and args.mode == 'write':
        if args.source is not None:
            ifNotAvroThenLoadRaw(str(args.source))
        else:
            for dataFolder in dataFolders:
                ifNotAvroThenLoadRaw(dataFolder)
            ifNotAvroThenLoadRaw(apiOption) #API
    if args.format == 'parquet' and args.mode == 'write':
        if args.source is not None:
            ifNotParquetThenLoadRaw(str(args.source))
        else:
            for dataFolder in dataFolders:
                ifNotParquetThenLoadRaw(dataFolder)
            ifNotParquetThenLoadRaw(apiOption)
    if args.format == 'raw' and args.mode == 'write':
        if args.source is not None:
            writeRaw(str(args.source))
        else:
            for dataFolder in dataFolders:
                writeRaw(dataFolder)
            writeRaw(apiOption)


    if args.format == 'avro' and args.mode == 'read':
        readAvro(str(args.readFile))
    if args.format == 'parquet' and args.mode == 'read':
        readParquet(str(args.readFile))

