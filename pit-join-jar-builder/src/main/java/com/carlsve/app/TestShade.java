package com.carlsve.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestShade {
    public static void addJob(StreamTableEnvironment tableEnv) {

        // SHUFFLE_MERGE directive forces SortMergeJoinFunction to manage the join, which we have shaded with PIT join correctness functionality
        Table result = tableEnv.sqlQuery(
            "SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 LEFT JOIN t2 ON t1.part_col = t2.part_col AND t1.id = t2.id AND t1.ts >= t2.ts"
        );

        result.printExplain();
        result.printSchema();
        result.execute().print();
    }
}
