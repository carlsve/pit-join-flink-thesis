import sys
import pandas as pd
import pyarrow.parquet as pq

filepath = sys.argv[1]

df = pd.read_parquet(filepath, engine='pyarrow')
print(
    df.describe(),
    df.dtypes,
    df.columns
)
print(
    df.groupby('part_col').size(),
    df['part_col'].values()
)
print(
    "Compression type:",
    pq.ParquetFile(filepath).metadata.row_group(0).column(0).compression
)
