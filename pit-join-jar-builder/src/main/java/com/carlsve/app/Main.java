package com.carlsve.app;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.carlsve.app.datagen.DataGen;

public class Main {
    public static void main(String ...args ) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamTableEnvironment tableEnv = DataGen.fromParquetGeneric(env);
        ExplodingJoin.addJob(tableEnv);
    }
}