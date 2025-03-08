import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os
import uuid

# must take ordered set
# must know size of set
# number of unique ids:
# lets use a join....

print(sys.argv)
unique_ids = int(sys.argv[1])
partition_cols = 100

input_dir = sys.argv[2]
output_dir = sys.argv[3]

left_input_dir = f"{input_dir}/left"
right_input_dir = f"{input_dir}/right"

left_df = pd.read_parquet(left_input_dir, engine='pyarrow')
right_df = pd.read_parquet(right_input_dir, engine='pyarrow')

left_df['part_col'] = left_df.index // partition_cols

id_part_df = left_df[['id', 'part_col']]
right_df = right_df.merge(id_part_df, on='id', how='left')

left_output_dir = f"{output_dir}/left/"
right_output_dir = f"{output_dir}/right/"

try:
    os.makedirs(left_output_dir)
    os.makedirs(right_output_dir)
except FileExistsError:
    print(f"MAKEDIR ERROR: {left_output_dir} or {right_output_dir} already exists")

left_dataset = pa.Table.from_pandas(left_df)
pq.write_to_dataset(left_dataset, root_path=left_output_dir, partition_cols=['part_col'])
right_dataset = pa.Table.from_pandas(right_df)
pq.write_to_dataset(right_dataset, root_path=right_output_dir, partition_cols=['part_col'])
