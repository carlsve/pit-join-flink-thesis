CREATE TABLE datagen_source (
    id STRING NOT NULL,
    ts INT NOT NULL,
    label STRING NOT NULL
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '<num_ids>',

    'fields.ts.min' = '31536000',
    'fields.ts.max' = '31536000'
);

CREATE TABLE t1 (
    id STRING NOT NULL,
    ts INT NOT NULL,
    label STRING NOT NULL
) WITH (
    'connector' = 'filesystem',
    'path' = './parquet_data/test_<num_ids>_<num_events_mean>_<num_events_std>/left',
    'format' = 'parquet'
);

CREATE TABLE t2 (
    id STRING NOT NULL,
    ts INT NOT NULL,
    state STRING NOT NULL
) WITH (
    'connector' = 'filesystem',
    'path' = './parquet_data/test_<num_ids>_<num_events_mean>_<num_events_std>/right',
    'format' = 'parquet'
);

INSERT INTO t1
SELECT * FROM datagen_source;

INSERT INTO t2
SELECT
  _id as id,
  _ts as ts,
  _state as state
FROM t1
LEFT JOIN LATERAL TABLE(GenValuesFromId(id, ts, label)) AS T(_id, _ts, _state) ON TRUE