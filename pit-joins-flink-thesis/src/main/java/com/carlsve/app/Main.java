package com.carlsve.app;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.carlsve.app.datagen.DataGen;

public class Main {
    public static void main(String ...args) throws Exception
    {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setRuntimeMode(RuntimeExecutionMode.BATCH);

      SingletonConfig conf = SingletonConfig.getInstance();
      conf.put("num_ids",         args.length >= 3 ? args[2] : "10000");
      conf.put("num_events_mean", args.length >= 4 ? args[3] : "20");
      conf.put("num_events_std",  args.length >= 5 ? args[4] : "2");
      
      StreamTableEnvironment tableEnv = null;

      // Actual job run
      if (args.length == 0) {
        tableEnv = DataGen.fromPartitioned(env);
        PitJoin.addJob(tableEnv);
      }

      String source = args.length >= 2 ? args[1] : "PARTITIONED";
      switch(source) {
        case "NEW": {
          tableEnv = DataGen.newData(env);
          break;
        }
        case "TEST": {
          tableEnv = DataGen.fromLegacy(env);
          break;
        }
        case "PARQUET": {
          tableEnv = DataGen.fromParquet(env);
          break;
        }
        case "DATAGEN": {
          tableEnv = DataGen.fromDataGen(env);
          break;
        }
        case "PARTITIONED": {
          tableEnv = DataGen.fromPartitioned(env);
          break;
        }
        default: {
          System.out.println("Wrong source, select TEST, PARQUET, DATAGEN, PARTITIONED or NEW");
          return;
        }
      }

      String job = args.length >= 1 ? args[0] : "PITJOIN";
      switch(job) {
        case "UNION": {
          UnionJoin.addJob(tableEnv);
          break;
        }
        case "PITJOIN": {
          PitJoin.addJob(tableEnv);
          break;
        }
        case "EXPLODING": {
          ExplodingJoin.addJob(tableEnv);
          break;
        }
        case "VIEW": {
          tableEnv.executeSql("SELECT * FROM t1 LIMIT 10").print();
          tableEnv.executeSql("SELECT COUNT(*) FROM t1").print();
          tableEnv.executeSql("SELECT * FROM t2 LIMIT 10").print();
          tableEnv.executeSql("SELECT COUNT(*) FROM t2").print();
          break;
        }
        default: {
          System.out.println("Wrong arg, choose: PITJOIN, EXPLODING, UNION or VIEW");
        }
      }
    }
}
