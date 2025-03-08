package com.carlsve.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class ExplodingJoin {
    public static void addJob(StreamTableEnvironment tableEnv) {
        Table combined = tableEnv.sqlQuery(
            "SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t1.ts >= t2.ts"
        );
        tableEnv.createTemporaryView("combined", combined);

        Table result = tableEnv.sqlQuery(
            "SELECT *, ROW_NUMBER() over w as rn FROM combined" +
            " WINDOW w AS (PARTITION BY id, ts ORDER BY ts0 DESC)"
        )
            .where($("rn").isEqual(1));

        result.printExplain();
        result.printSchema();
        result.execute().print();
    }
}
