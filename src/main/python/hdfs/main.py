from writer.avroWriter import writeAvro
from writer.parquetWriter import writeParquet
from reader.avroReader import readAvro
from reader.parquetReader import readParquet
import argparse

parser = argparse.ArgumentParser()
writeGroup =  parser.add_mutually_exclusive_group()
parser.add_argument('mode', choices=['write', 'read'], help='Choose write or read mode')
parser.add_argument('format', choices=['avro', 'parquet'], help='Choose avro or parquet formats')
parser.add_argument('-i', '--inputOption', choices=['income','property','lookup'], help='Input file options: {income, property, lookup}')
parser.add_argument('-o', '--outputName', help='Output file name. Avoid adding .avro extension. E.g., outputName will generate outputName.avro file')
parser.add_argument('-r', '--readFile', help='Reading file path.')

args = parser.parse_args()

if args.outputName is not None and '.' in args.outputName:
    raise ValueError("Invalid outputName argument provided. The output name must not contain any extension")
if (not args.readFile.endswith(".avro") and args.format == "avro") or (not args.readFile.endswith(".parquet") and args.format == "parquet"):
    raise ValueError("Invalid file type. The file type  must coincide with the format you are reading (e.g. '.parquet' for parquet format and '.avro' for avro format)")

if __name__ == '__main__':
    if args.format =='avro' and args.mode == 'write':
        writeAvro(str(args.inputOption), str(args.outputName))
    if args.format =='parquet' and args.mode == 'write':
        writeParquet(str(args.inputOption))
    if args.format == 'avro' and args.mode == 'read':
        readAvro(str(args.readFile))
    if args.format == 'parquet' and args.mode == 'read':
        readParquet(str(args.readFile))


