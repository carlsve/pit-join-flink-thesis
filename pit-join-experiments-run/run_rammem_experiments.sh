# Assume taskmanagers active:
JOIN_TYPE=$1
DATASET_PATH="../pit-joins-flink-thesis/datasets/unsorted/rows_per_cid_1/100000_80_8"
RAMMEM_RESULT_PATH="./results_rammem/${JOIN_TYPE}"


mv "${DATASET_PATH}/left" ./parquet_data/current
mv "${DATASET_PATH}/right" ./parquet_data/current

if [[ "$JOIN_TYPE" == "pit" ]]; then
    echo "copying jars for pit-run"
    cp ./unpartitioned_smj/flink-table-runtime-1.17.2.jar ../flink-1.17.2/lib/flink-table-runtime-1.17.2.jar
    cp ./unpartitioned_smj/flink-table-planner_2.12-1.17.2.jar ../flink-1.17.2/opt/flink-table-planner_2.12-1.17.2.jar
fi

../flink-1.17.2/bin/start-cluster.sh
mkdir -p $RAMMEM_RESULT_PATH

../flink-1.17.2/bin/flink run ../pit-join-jar-builder/jars/unpartitioned/${JOIN_TYPE}_join.jar

curl localhost:8081/jobs/overview > "${RAMMEM_RESULT_PATH}/overview.json"
for i in $(jq '.jobs[] | .jid' "${RAMMEM_RESULT_PATH}/overview.json"); do
    temp="${i%\"}"
    temp="${temp#\"}"
    curl "localhost:8081/jobs/${temp}" > "${RAMMEM_RESULT_PATH}/${temp}.json"
done

mv ./parquet_data/current/left $DATASET_PATH
mv ./parquet_data/current/right $DATASET_PATH

cp ./normal_smj/flink-table-runtime-1.17.2.jar ../flink-1.17.2/lib/flink-table-runtime-1.17.2.jar
cp ./normal_smj/flink-table-planner_2.12-1.17.2.jar ../flink-1.17.2/opt/flink-table-planner_2.12-1.17.2.jar

../flink-1.17.2/bin/stop-cluster.sh
