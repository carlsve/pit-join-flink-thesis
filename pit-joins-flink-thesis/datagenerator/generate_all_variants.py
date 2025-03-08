import sys
import os
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from uuid import uuid4


#
# sys.argv: [, unique_ids, state_mean, state_std, root_path]
#
unique_ids, state_mean, state_std, root_path = sys.argv[1:]
print("### ARGS ###")
print(unique_ids, state_mean, state_std, root_path)

max_ts = 31536000
partition_cols_1 = int(unique_ids) // 100
partition_cols_5 = 5 * int(unique_ids) // 100

def write_dfs(left_df, right_df, output_dir, part_cols=None):
    left_path = f"{output_dir}/{unique_ids}_{state_mean}_{state_std}/left"
    print(left_df)
    print(left_df.dtypes)
    print(left_path)
    print("writing left...")
    left_dataset = pa.Table.from_pandas(left_df)
    pq.write_to_dataset(left_dataset, root_path=left_path, compression='gzip', partition_cols=part_cols)
    
    right_path = f"{output_dir}/{unique_ids}_{state_mean}_{state_std}/right"
    print(right_df)
    print(right_df.dtypes)
    print(right_path)
    print("writing right...")
    right_dataset = pa.Table.from_pandas(right_df)
    pq.write_to_dataset(right_dataset, root_path=right_path, compression='gzip', partition_cols=part_cols)


#
# create initial ur-table
#
df = pd.DataFrame({
    'id': [str(uuid4()) for _ in range(int(unique_ids))],
    'ts': 31536000
})
df['ts'] = df['ts'].astype('int32')
print("ur table:")
print(df)
print(df.dtypes)

#
# generate left table, from which the rest of all variants will be spawned
#
left_df = df.copy()
left_df['label'] = left_df['id'].map(lambda r: r[:8])

#
# generate right table, from which the rest of all variants will be spawned
#
right_df = left_df.copy()
right_df['repeats'] = right_df.index.map(lambda _: round(np.random.normal(int(state_mean), int(state_std))))
right_df = right_df.loc[right_df.index.repeat(right_df['repeats'])]
right_df['state'] = right_df.label.map(lambda l: f"{l}:{str(uuid4())[:8]}")
right_df['row_number'] = right_df.groupby('id').cumcount()
right_df['ts'] = right_df.row_number.map(lambda rn: max_ts - 60 * (rn + 1))
right_df['ts'] = right_df['ts'].astype('int32')

right_df = right_df.drop(['repeats', 'label', 'row_number'], axis=1)
right_df = right_df.reset_index(drop=True)


#
#
# WRITING SECTION OF SCRIPT
#
#


#
# generate sorted unpartitioned dataset with 1 rows per cid in left table
#
print('generating sorted unpartitioned dataset with 1 rows per cid in left table...')

write_dfs(left_df, right_df, f"{root_path}/unpartitioned/sorted/rows_per_cid_1")

#
# generate sorted unpartitioned dataset with 5 rows per cid in left table
#
print('generating sorted unpartitioned dataset with 5 rows per cid in left table...')
L = [max_ts, *[max_ts - int(abs(n)) for n in np.random.normal(5*60, 60, size=4)]]
L.sort()

left_df_5 = (
    pd.DataFrame(np.repeat(left_df.values, 5, axis=0), columns=left_df.columns)
    .assign(ts = np.tile(L, len(left_df)))
).astype({'ts': 'int32'})
right_df_5 = right_df.copy()

write_dfs(left_df_5, right_df_5, f"{root_path}/unpartitioned/sorted/rows_per_cid_5")

#
# generate sorted partitioned dataset with 1 rows per cid in left table
#
print('generating sorted partitioned dataset with 1 rows per cid in left table...')

left_df_part = left_df.copy()
right_df_part = right_df.copy()
left_df_part['part_col'] = left_df_part.index // partition_cols_1
left_df_part = left_df_part.astype({'part_col': 'int32'})

id_part_df = left_df_part[['id', 'part_col']]
right_df_part = right_df_part.merge(id_part_df, on='id', how='left')

write_dfs(left_df_part, right_df_part, f"{root_path}/partitioned/sorted/rows_per_cid_1", part_cols=['part_col'])

#
# generate sorted partitioned dataset with 5 rows per cid in left table
#
print('generating sorted partitioned dataset with 5 rows per cid in left table...')

left_df_5_part = left_df_5.copy()
right_df_5_part = right_df.copy()
left_df_5_part['part_col'] = left_df_5_part.index // partition_cols_5
left_df_5_part = left_df_5_part.astype({'part_col': 'int32'})
id_part_df = left_df_5_part[['id', 'part_col']]
right_df_5_part = right_df_5_part.merge(id_part_df, on='id', how='left')

write_dfs(left_df_5_part, right_df_5_part, f"{root_path}/partitioned/sorted/rows_per_cid_5", part_cols=['part_col'])

#
# generate unsorted unpartitioned dataset with 1 rows per cid in left table
#
print('generating unsorted unpartitioned dataset with 1 rows per cid in left table...')

left_df = left_df.iloc[np.random.permutation(len(left_df))]
left_df = left_df.reset_index(drop=True)
right_df = right_df.iloc[np.random.permutation(len(right_df))]
right_df = right_df.reset_index(drop=True)

write_dfs(left_df, right_df, f"{root_path}/unpartitioned/unsorted/rows_per_cid_1")

#
# generate unsorted unpartitioned dataset with 5 rows per cid in left table
#
print('generating unsorted unpartitioned dataset with 5 rows per cid in left table...')

left_df_5 = left_df_5.iloc[np.random.permutation(len(left_df_5))]
left_df_5 = left_df_5.reset_index(drop=True)
right_df_5 = right_df_5.iloc[np.random.permutation(len(right_df))]
right_df_5 = right_df_5.reset_index(drop=True)

write_dfs(left_df_5, right_df_5, f"{root_path}/unpartitioned/unsorted/rows_per_cid_5")
