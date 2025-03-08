import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os
import uuid

input_table_path = f"{sys.argv[1]}"
output_table_path = f"{sys.argv[2]}"

try:
    os.makedirs(output_table_path)
except FileExistsError:
    print(f"{output_table_path} already exists")

df = pd.read_parquet(input_table_path, engine='pyarrow')
print(df, df.dtypes, len(df), df.columns, df.axes)
df = df.iloc[np.random.permutation(len(df))]
print(df, df.dtypes, len(df), df.columns, df.axes)
df = df.reset_index(drop=True)
print(df, df.dtypes, len(df), df.columns, df.axes)

table = pa.Table.from_pandas(df)
pq.write_table(table, f"{output_table_path}/part-{str(uuid.uuid4())}.parquet")
