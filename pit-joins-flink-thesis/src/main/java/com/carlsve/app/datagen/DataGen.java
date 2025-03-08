package com.carlsve.app.datagen;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import com.carlsve.app.ExplodingJoin;
import com.carlsve.app.SingletonConfig;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class DataGen {
    /*
     * newData will run setup_tables.sql, create datasource, table t1 and t2, fill t1 and t2 from datasource and save to parquet
     */
    public static StreamTableEnvironment newData(StreamExecutionEnvironment env) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.createTemporarySystemFunction("GenValuesFromId", GenValuesFromId.class);
        String[] statements = getSqlFromPath("datagen/setup_tables.sql").split(";");
        for (String statement : statements) {
            tableEnv.executeSql(statement).await();
        }

        return tableEnv;
    }

    public static StreamTableEnvironment fromLegacy(StreamExecutionEnvironment env) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        final LogicalType[] fieldTypes = new LogicalType[] { new VarCharType(), new BigIntType(), new VarCharType() };
                
        // Setup left stream
        final RowType rowType1 = RowType.of(fieldTypes, new String[] {"id", "ts", "label"});
        final ParquetColumnarRowInputFormat<FileSourceSplit> format1 =
        new ParquetColumnarRowInputFormat<>(new Configuration(), rowType1, InternalTypeInfo.of(rowType1), 1, false, true);
        Path path1 = new Path(ExplodingJoin.class.getClassLoader().getResource("left_test_table.parquet").getPath());
        final FileSource<RowData> source1 = FileSource.forBulkFileFormat(format1, path1).build();
        final DataStream<RowData> stream1 = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "source1");
        
        Table t1 = tableEnv.fromDataStream(stream1);
        tableEnv.createTemporaryView("t1", t1);

        // Setup right stream
        final RowType rowType2 = RowType.of(fieldTypes, new String[] {"id", "ts", "state"});
        final ParquetColumnarRowInputFormat<FileSourceSplit> format2 =
        new ParquetColumnarRowInputFormat<>(new Configuration(), rowType2, InternalTypeInfo.of(rowType2), 1, false, true);
        Path path2 = new Path(ExplodingJoin.class.getClassLoader().getResource("right_test_table.parquet").getPath());
        final FileSource<RowData> source2 = FileSource.forBulkFileFormat(format2, path2).build();
        final DataStream<RowData> stream2 = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "source2");

        Table t2 = tableEnv.fromDataStream(stream2);
        tableEnv.createTemporaryView("t2", t2);

        return tableEnv;
    }

    public static StreamTableEnvironment fromParquet(StreamExecutionEnvironment env) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql(getSqlFromPath("datagen/gen_left_table.sql")).await();
        tableEnv.executeSql(getSqlFromPath("datagen/gen_right_table.sql")).await();
        return tableEnv;
    }

    public static StreamTableEnvironment fromParquetGeneric(StreamExecutionEnvironment env) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String leftTableSql =
            "CREATE TABLE t1 (\n" +
            "    id STRING NOT NULL,\n" +
            "    ts INT NOT NULL,\n" +
            "    label STRING NOT NULL\n" +
            ") WITH (\n" +
            "    'connector' = 'filesystem',\n" +
            "    'path' = './parquet_data/current/left',\n" +
            "    'format' = 'parquet'\n" +
            ")";
            
        String rightTableSql =
            "CREATE TABLE t2 (\n" +
            "    id STRING NOT NULL,\n" +
            "    ts INT NOT NULL,\n" +
            "    state STRING NOT NULL\n" +
            ") WITH (\n" +
            "    'connector' = 'filesystem',\n" +
            "    'path' = './parquet_data/current/right',\n" +
            "    'format' = 'parquet'\n" +
            ")";
     
        tableEnv.executeSql(leftTableSql).await();
        tableEnv.executeSql(rightTableSql).await();

        return tableEnv;
    }

    public static StreamTableEnvironment fromDataGen(StreamExecutionEnvironment env) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.createTemporarySystemFunction("GenValuesFromId", GenValuesFromId.class);
        tableEnv.executeSql(getSqlFromPath("datagen/datagen_source_table.sql")).await();
        Table t1 = tableEnv.sqlQuery("SELECT * FROM datagen_source");
        tableEnv.createTemporaryView("t1", t1);
        Table t2 = tableEnv.sqlQuery(
            "SELECT _id as id, _ts as ts, _state as state " +
            "FROM datagen_source " +
            "LEFT JOIN LATERAL TABLE(GenValuesFromId(id, ts, label)) AS T(_id, _ts, _state) ON TRUE"
        );

        
        tableEnv.createTemporaryView("t2", t2);

        return tableEnv;
    }

    public static StreamTableEnvironment fromPartitioned(StreamExecutionEnvironment env) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String leftTableSql =
            "CREATE TABLE t1 (\n" +
            "    id STRING NOT NULL,\n" +
            "    part_col INT NOT NULL,\n" +
            "    ts INT NOT NULL,\n" +
            "    label STRING NOT NULL\n" +
            ") PARTITIONED BY (part_col) WITH (\n" +
            "    'connector' = 'filesystem',\n" +
            "    'path' = './parquet_data/current/left',\n" +
            "    'format' = 'parquet'\n" +
            ")";
            
        String rightTableSql =
            "CREATE TABLE t2 (\n" +
            "    id STRING NOT NULL,\n" +
            "    part_col INT NOT NULL,\n" +
            "    ts INT NOT NULL,\n" +
            "    state STRING NOT NULL\n" +
            ") PARTITIONED BY (part_col) WITH (\n" +
            "    'connector' = 'filesystem',\n" +
            "    'path' = './parquet_data/current/right',\n" +
            "    'format' = 'parquet'\n" +
            ")";
     
        tableEnv.executeSql(leftTableSql).await();
        tableEnv.executeSql(rightTableSql).await();

        return tableEnv;
    }

    @FunctionHint(output = @DataTypeHint("ROW<_id STRING, _ts INT, _state STRING>"))
    public static class GenValuesFromId extends TableFunction<Row> {
        Random random = new Random();

        SingletonConfig conf = SingletonConfig.getInstance();
        double mean = Double.parseDouble(conf.get("num_events_mean"));
        double std = Double.parseDouble(conf.get("num_events_std"));

        public void eval(String id, int ts, String label) {
            int numEvents =  (int) (random.nextGaussian() * std + mean);
            for (int i = 0; i < numEvents; ++i) {
                collect(Row.of(id, ts - 60*(i + 1), id.substring(0, 5) + ":" + ts + ":" + label));
            }
        }
    }

    public static String getSqlFromPath(String path) throws Exception {
        Path loadPath = new Path(DataGen.class.getClassLoader().getResource(path).getPath());
        String templateSql = new String(Files.readAllBytes(Paths.get(loadPath.getPath())));

        SingletonConfig conf = SingletonConfig.getInstance();
        for (String key : conf.keySet()) {
            templateSql = templateSql.replaceAll("<"+key+">", conf.get(key));
        }

        return templateSql;
    }
}
