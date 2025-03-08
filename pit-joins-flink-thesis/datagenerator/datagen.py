import argparse
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid

def datagen(unique_ids, maximum_timestamp, events_per_id):
    left_data = []
    right_data = []
    for i in range(unique_ids):
        left_data.append({
            'id': str(i),
            'timestamp': maximum_timestamp,
            'label': str(uuid.uuid4())
        })
    
        num_values_per_label = int(np.random.normal(events_per_id['mean'], events_per_id['std']))
        for j in range(num_values_per_label):
            event_time = (j/num_values_per_label) * maximum_timestamp
            right_data.append({
                'id': str(i),
                'timestamp': event_time,
                'value': str(uuid.uuid4())
            })

    left_df = pd.DataFrame(left_data)
    right_df = pd.DataFrame(right_data)
    left_table = pa.Table.from_pandas(left_df)
    right_table = pa.Table.from_pandas(right_df)

    pq.write_table(left_table, f'./left_table_{unique_ids}_{events_per_id["mean"]}_{events_per_id["std"]}.parquet')
    pq.write_table(right_table, f'./right_table_{unique_ids}_{events_per_id["mean"]}_{events_per_id["std"]}.parquet')

if __name__ == '__main__':
    unique_ids = [10_000, 100_000, 1_000_000, 10_000_000]
    maximum_timestamp = 60 * 60 * 24 * 365
    events_per_id = [
        { 'mean': 20, 'std': 2 },
        { 'mean': 80, 'std': 8 }
    ]

    left_table_keys = ['id', 'timestamp', 'label']
    right_table_keys = ['id', 'timestamp', 'value']

    datagen(unique_ids[0], maximum_timestamp, events_per_id[0])
