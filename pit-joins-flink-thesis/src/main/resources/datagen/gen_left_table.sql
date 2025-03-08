CREATE TABLE t1 (
    id STRING NOT NULL,
    ts INT NOT NULL,
    label STRING NOT NULL
) WITH (
    'connector' = 'filesystem',
    'path' = './parquet_data/test_<num_ids>_<num_events_mean>_<num_events_std>/left',
    'format' = 'parquet'
)