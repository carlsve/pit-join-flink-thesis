"""
TODO:
- HOT_KEY functionality
- normal distributions
- sorted/unsorted events
- sorted/unsorted ids
"""


import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df = pd.DataFrame({
    "n": [n for n in range(1000)]
})



table = pa.Table.from_pandas(df)
pq.write_table(table, 'table.parquet')