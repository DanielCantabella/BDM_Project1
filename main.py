from src.writer.avroWriter import writeAvro
from src.writer.parquetWriter import writeParquet
from src.writer.sequenceFileWriter import writeSeq
from src.reader.avroReader import readAvro
from src.reader.parquetReader import readParquet
from src.reader.sequenceReader import readSeq
import argparse

parser = argparse.ArgumentParser()
# writeGroup = parser.add_mutually_exclusive_group()
parser.add_argument('mode', choices=['write', 'read'], help='Choose write or read mode')
parser.add_argument('format', choices=['avro', 'parquet', 'sequence'], help='Choose avro or parquet formats')
parser.add_argument('-i', '--inputOption', choices=['income','property','lookup'], help='Input file options: {income, property, lookup}')
parser.add_argument('-r', '--readFile', help='Reading file path.')

args = parser.parse_args()

# if args.outputName is not None and '.' in args.outputName:
#     raise ValueError("Invalid outputName argument provided. The output name must not contain any extension")
# if (not args.readFile.endswith(".avro") and args.format == "avro") or (not args.readFile.endswith(".parquet") and args.format == "parquet"):
#     raise ValueError("Invalid file type. The file type  must coincide with the format you are reading (e.g. '.parquet' for parquet format and '.avro' for avro format)")

if __name__ == '__main__':
    if args.format == 'avro' and args.mode == 'write':
        writeAvro(str(args.inputOption))
    if args.format == 'parquet' and args.mode == 'write':
        writeParquet(str(args.inputOption))
    if args.format == 'sequence' and args.mode == 'write':
        writeSeq(str(args.inputOption))
    if args.format == 'avro' and args.mode == 'read':
        readAvro(str(args.readFile))
    if args.format == 'parquet' and args.mode == 'read':
        readParquet(str(args.readFile))
    if args.format == 'sequence' and args.mode == 'read':
        readSeq(str(args.readFile))


