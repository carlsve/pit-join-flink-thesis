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

# HOT_KEY flag - let a select few id's have much larger event sets
HOT_KEY_ENABLE = False
HOT_KEY_FACTOR = 10

number_of_unique_ids = 10

number_of_events_per_id = 20
number_of_decisions_per_id = 5
maximum_timestamp = 1000

left_ids = [f"event_{id}" for id in range(number_of_unique_ids)]

_left_ids = []
left_timestamps = []
left_values = []

for e_id in left_ids:
    e_tss = np.sort(np.random.randint(0, maximum_timestamp, number_of_events_per_id))
    for e_ts in e_tss:
        _left_ids.append(e_id)
        left_timestamps.append(e_ts)
        left_values.append(f"{e_id}-{e_ts}")

left_df = pd.DataFrame({
    "id": _left_ids,
    "ts": left_timestamps,
    "vals": left_values
})

right_ids = [f"decision_{id}" for id in range(number_of_unique_ids)]
_right_ids = []
right_timestamps = []
right_values = []

for d_id in right_ids:
    d_tss = np.sort(np.random.randint(0, maximum_timestamp, number_of_decisions_per_id))

    for d_ts in d_tss:
        _right_ids.append(d_id)
        right_timestamps.append(d_ts)
        right_values.append(f"{d_id}-{d_ts}")


right_df = pd.DataFrame({
    "id": _right_ids,
    "ts": right_timestamps,
    "vals": right_values
})


left_table = pa.Table.from_pandas(left_df)
pq.write_table(left_table, 'left_table.parquet')

right_table = pa.Table.from_pandas(right_df)
pq.write_table(right_table, 'right_table.parquet')