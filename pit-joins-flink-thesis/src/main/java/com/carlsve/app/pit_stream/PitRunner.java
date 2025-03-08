package com.carlsve.app.pit_stream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PitRunner {
    public static void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        addJob(env);
        env.execute();
    }

    public static void addJob(StreamExecutionEnvironment env) {
        DataStream<RowPojo> stream1 = env.fromElements(
            new RowPojo("1", 4, "1z"),
            new RowPojo("1", 5, "1x"),
            new RowPojo("2", 6, "2x"),
            new RowPojo("1", 7, "1y"),
            new RowPojo("2", 8, "2y")
        );

        DataStream<RowPojo> stream2 = env.fromElements(
            new RowPojo("1", 1, "f3-1-1"),
            new RowPojo("2", 2, "f3-2-2"),
            new RowPojo("1", 6, "f3-1-6"),
            new RowPojo("2", 8, "f3-2-8"),
            new RowPojo("1", 10, "f3-1-10")
        );

        DataStream<String> result = stream1
            .connect(stream2)
            .transform(
                "pit",
                TypeInformation.of(new TypeHint<String> () {}),
                new PitOperator()
            );

        result.print();
    }
}
