
mv ../pit-joins-flink-thesis/parquet_data/test_10000_20_2 ../pit-joins-flink-thesis/parquet_data/current
./bin/flink run ../pit-join-jar-builder/jars/pit_join.jar
mv ../pit-joins-flink-thesis/parquet_data/current ../pit-joins-flink-thesis/parquet_data/test_10000_20_2

mv ../pit-joins-flink-thesis/parquet_data/test_10000_80_8 ../pit-joins-flink-thesis/parquet_data/current
./bin/flink run ../pit-join-jar-builder/jars/pit_join.jar
mv ../pit-joins-flink-thesis/parquet_data/current ../pit-joins-flink-thesis/parquet_data/test_10000_80_8

mv ../pit-joins-flink-thesis/parquet_data/test_100000_20_2 ../pit-joins-flink-thesis/parquet_data/current
./bin/flink run ../pit-join-jar-builder/jars/pit_join.jar
mv ../pit-joins-flink-thesis/parquet_data/current ../pit-joins-flink-thesis/parquet_data/test_100000_20_2

mv ../pit-joins-flink-thesis/parquet_data/test_100000_80_8 ../pit-joins-flink-thesis/parquet_data/current
./bin/flink run ../pit-join-jar-builder/jars/pit_join.jar
mv ../pit-joins-flink-thesis/parquet_data/current ../pit-joins-flink-thesis/parquet_data/test_100000_80_8

mv ../pit-joins-flink-thesis/parquet_data/test_1000000_20_2 ../pit-joins-flink-thesis/parquet_data/current
./bin/flink run ../pit-join-jar-builder/jars/pit_join.jar
mv ../pit-joins-flink-thesis/parquet_data/current ../pit-joins-flink-thesis/parquet_data/test_1000000_20_2

mv ../pit-joins-flink-thesis/parquet_data/test_1000000_80_8 ../pit-joins-flink-thesis/parquet_data/current
./bin/flink run ../pit-join-jar-builder/jars/pit_join.jar
mv ../pit-joins-flink-thesis/parquet_data/current ../pit-joins-flink-thesis/parquet_data/test_1000000_80_8
