"""
  +---+---+-----+  -  +---+---+-------+
  | id| ts|value|  -  | id| ts|  value|
  +---+---+-----+  -  +---+---+-------+
  |  1|  4|   1z|  -  |  1|  1| f3-1-1|
  |  1|  5|   1x|  -  |  2|  2| f3-2-2|
  |  2|  6|   2x|  -  |  1|  6| f3-1-6|
  |  1|  7|   1y|  -  |  2|  8| f3-2-8|
  |  2|  8|   2y|  -  |  1| 10|f3-1-10|
  +---+---+-----+  -  +---+---+-------+

  JOINCONDITIONS
  (id = id)
  (ts >= ts)
  ((id = id) AND (ts >= ts))
  COMBINED
  +---+---+-----+---+---+------+
  | id| ts|value| id| ts| value|
  +---+---+-----+---+---+------+
  |  1|  7|   1y|  1|  1|f3-1-1|
  |  1|  5|   1x|  1|  1|f3-1-1|
  |  1|  4|   1z|  1|  1|f3-1-1|
  |  2|  8|   2y|  2|  2|f3-2-2|
  |  2|  6|   2x|  2|  2|f3-2-2|
  |  1|  7|   1y|  1|  6|f3-1-6|
  |  2|  8|   2y|  2|  8|f3-2-8|
  +---+---+-----+---+---+------+
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

left_data = [
    { 'id': '1', 'ts': 4, 'label': '1z' },
    { 'id': '1', 'ts': 5, 'label': '1x' },
    { 'id': '2', 'ts': 8, 'label': '2y' },
    { 'id': '2', 'ts': 6, 'label': '2x' },
    { 'id': '1', 'ts': 7, 'label': '1y' },
]
left_df = pd.DataFrame(left_data)


right_data = [
    { 'id': '1', 'ts': 1, 'state': 'f3-1-1' },
    { 'id': '2', 'ts': 2, 'state': 'f3-2-2' },
    { 'id': '1', 'ts': 6, 'state': 'f3-1-6' },
    { 'id': '2', 'ts': 8, 'state': 'f3-2-8' },
    { 'id': '1', 'ts': 10, 'state': 'f3-1-10' }
]
right_df = pd.DataFrame(right_data)

left_table = pa.Table.from_pandas(left_df)
pq.write_table(left_table, './src/main/resources/left_test_table.parquet')

right_table = pa.Table.from_pandas(right_df)
pq.write_table(right_table, './src/main/resources/right_test_table.parquet')
