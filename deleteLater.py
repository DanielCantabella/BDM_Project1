import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Load the CSV file into a pandas DataFrame
df = pd.read_csv('data/opendatabcn-income/2007_Distribucio_territorial_renda_familiar.csv')

# Convert the pandas DataFrame to a pyarrow Table
table = pa.Table.from_pandas(df)

# Write the pyarrow Table to a Parquet file
pq.write_table(table, 'data.parquet')