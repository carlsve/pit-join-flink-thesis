# pit-join-flink-thesis
Source code for the thesis that creates a point-in-time correct join algorithm via the built in Sort-Merge Join in Apache Flink

### How to run:
Follow the guide, download the cluster for Flink 1.17.2
https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/try-flink/local_installation/

!!!!!!!!!
All of the jars used in the final experiments are included in the repo for convenience's sake.
You can email me if you want me to add a branch where the jars aren't included.

Note: All job jars have been zipped to get around github's file size limit.
      They need to be manually unzipped
!!!!!!!!!

Copy over pit-join-experiments-run/{normal_smj|partitioned_smj}/flink-table-runtime.1.17.2.jar
to .../flink-1.17.2/lib/

and pit-join-experiments-run/{normal_smj|partitioned_smj}/flink-table-planner_2.12-1.17.2.jar
to .../flink-1.17.2/opt/

As of right now, the jobs that run the modified code are hard coded to take data generated via datagen, from the file system.
Look in pit-joins-flink-thesis/Makefile for data generation.

to run the jobs, perform the following steps:
```
> .../flink-1.17.2/bin/start-cluster.sh
> .../flink-1.17.2/bin/flink run pit-join-jar-builder/{partitioned|unpartitioned}/pit_join.jar
> .../flink-1.17.2/bin/start-cluster.sh
```

The experiments are messy to say the least. Look at them for reference rather than for a fully perfect happy path.

### Location of the Modified Sort-Merge Join
The modified Sort-Merge Join code is in:
```
pit-joins-flink-thesis/
  src/main/java/org/apache/flink/table/runtime/operators/join
```

You can compile it with maven to run tests in this project.

If you want to run it inside the proper apache flink cluster, you need to recompile flink-table-runtime Apache Flink (1.17.2) with both files copied over, and then copy over the jar files to the flink cluster.

### PIT_Stream
There is also a job called pit_stream, it's not relevant for the thesis, more an exploratory example.
