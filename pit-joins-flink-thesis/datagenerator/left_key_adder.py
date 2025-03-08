import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import uuid

left_table_path = f"./parquet_data_for_test_set/{sys.argv[1]}/left"
df = pd.read_parquet(left_table_path, engine='pyarrow')

L = [31536000, *[31536000 - int(abs(n)) for n in np.random.normal(5*60, 60, size=4)]]
L.sort()

new_df = (pd.DataFrame(np.repeat(df.values, 5, axis=0), columns=df.columns)
    .assign(ts = np.tile(L, len(df)))).astype({'ts': 'int32'})

table = pa.Table.from_pandas(new_df)
pq.write_table(table, f'./parquet_data/{sys.argv[1]}/left/part-{str(uuid.uuid4())}.parquet')
