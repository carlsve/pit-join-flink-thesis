CREATE TABLE datagen_source (
    id STRING NOT NULL,
    ts INT NOT NULL,
    label STRING NOT NULL
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '<num_ids>',

    'fields.ts.min' = '31536000',
    'fields.ts.max' = '31536000'
)
