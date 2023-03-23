import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
def readAvro(fileName):
    with open(fileName, 'rb') as avro_file:
        reader = DataFileReader(avro_file, DatumReader())
        for record in reader:
            print(record)
        reader.close()
