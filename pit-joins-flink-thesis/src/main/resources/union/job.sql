                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                SELECT id, ts, label, CAST(NULL as VARCHAR) as id0, CAST() FROM 

--Table t1WithDfIndex = tableEnv.sqlQuery("SELECT *, CAST(NULL as VARCHAR) as id0, CAST(NULL as INT) as ts0, CAST(NULL as VARCHAR) as state0, 1 as index_df FROM t1");
--tableEnv.createTemporaryView("t1WithDfIndex", t1WithDfIndex);
--
--Table t2WithDfIndex = tableEnv.sqlQuery("SELECT CAST(NULL as VARCHAR) as id, CAST(NULL as INT) as ts, CAST(NULL as VARCHAR) as label, id as id0, ts as ts0, state as state0, 0 as index_df FROM t2");
--tableEnv.createTemporaryView("t2WithDfIndex", t2WithDfIndex);
--
--Table combined = tableEnv.sqlQuery(
--  "SELECT *, COALESCE(ts, ts0) as combined_ts, COALESCE(id, id0) as combined_id FROM ((SELECT * FROM t1WithDfIndex) UNION ALL (SELECT * FROM t2WithDfIndex))"
--);
--tableEnv.createTemporaryView("combined", combined);
--
--Table combinedid = tableEnv.sqlQuery(
--  "SELECT *, last_value(id0) over w as id1 FROM combined" +
--  " WINDOW w AS (PARTITION BY combined_id ORDER BY combined_ts, index_df ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
--);
--tableEnv.createTemporaryView("combinedid", combinedid);
--
--Table combinedts = tableEnv.sqlQuery(
--  "SELECT *, last_value(ts0) over w as ts1 FROM combinedid" +
--  " WINDOW w AS (PARTITION BY combined_id ORDER BY combined_ts, index_df ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
--);
--tableEnv.createTemporaryView("combinedts", combinedts);
--
--Table combinedfull = tableEnv.sqlQuery(
--  "SELECT *, last_value(state0) over w as state1 FROM combinedts" +
--  " WINDOW w AS (PARTITION BY combined_id ORDER BY combined_ts, index_df ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
--);
--tableEnv.createTemporaryView("combinedfull", combinedfull);
--Table result = combinedfull.where($("ts").isNotNull()).limit(20);
--
--result.printExplain();
--result.printSchema();
--result.execute().print();