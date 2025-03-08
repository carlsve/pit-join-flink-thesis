package com.carlsve.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.coalesce;

public class UnionJoin {
    public static void addJob(StreamTableEnvironment tableEnv) {
        Table t1Prefixed = tableEnv.sqlQuery("SELECT id, part_col, ts as l_ts, label as l_label, CAST(NULL as INT) as r_ts, CAST(NULL as VARCHAR) as r_state, 1 as index_df FROM t1");
        Table t2Prefixed = tableEnv.sqlQuery("SELECT id, part_col, CAST(NULL as INT) as l_ts, CAST(NULL as VARCHAR) as l_label, ts as r_ts, state as r_state, 0 as index_df FROM t2");
        
        Table combined = t1Prefixed.union(t2Prefixed)
          .addColumns(coalesce($("l_ts"), $("r_ts")).as("combined_ts"));
        tableEnv.createTemporaryView("combined", combined);  

        Table combinedTS = tableEnv.sqlQuery(
            "SELECT id, part_col, l_ts, l_label, last_value(r_ts) over w as r_ts, r_state, index_df, combined_ts FROM combined" +
            " WINDOW w AS (PARTITION BY part_col, id ORDER BY combined_ts, index_df ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        );
        tableEnv.createTemporaryView("combinedTS", combinedTS);  

        Table combinedState = tableEnv.sqlQuery(
            "SELECT id, part_col, l_ts, l_label, r_ts, last_value(r_state) over w as r_state, index_df, combined_ts FROM combinedTS" +
            " WINDOW w AS (PARTITION BY part_col, id ORDER BY combined_ts, index_df ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        );

        Table result = combinedState
            .filter($("r_ts").isNotNull())
            .filter($("l_ts").isNotNull())
            .dropColumns($("index_df"))
            .dropColumns($("combined_ts"));

        result.execute().print();
    }
}
