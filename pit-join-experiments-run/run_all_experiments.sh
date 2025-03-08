# For all join types
    # For all parallelisms
        # For all ids
            # For all variations
                # Run unpartitioned sorted
                # Run unpartitioned unsorted
                # Run partitioned sorted

CONFIG="config.json"

jq -c '.join_types[]' $CONFIG | while read JOIN_TYPE; do
    temp="${JOIN_TYPE%\"}"
    temp="${temp#\"}"
    JOIN_TYPE=$temp
    jq -c '.keys_per_id_left[]' $CONFIG | while read KEYS_PER_ID; do
        cp ./normal_smj/flink-table-runtime-1.17.2.jar ../flink-1.17.2/lib/flink-table-runtime-1.17.2.jar
        cp ./normal_smj/flink-table-planner_2.12-1.17.2.jar ../flink-1.17.2/opt/flink-table-planner_2.12-1.17.2.jar

        jq -c '.parallelism[]' $CONFIG | while read PARALLELISM; do
            # Set parallelism in flink/conf
            # sed -E -i '' "s/parallelism.default: [[:digit:]]/parallelism.default: ${PARALLELISM}/g" ../flink-1.17.2/conf/flink-conf.yaml
            
            jq -c '.ids[]' $CONFIG | while read IDS; do
                jq -c '.variations[]' $CONFIG | while read VARIATIONS; do
                    temp="${VARIATIONS%\"}"
                    temp="${temp#\"}"
                    VARIATIONS=$temp
                    GENERIC_RESULTS_PATH="${JOIN_TYPE}/keys_per_id_${KEYS_PER_ID}/parallelism_${PARALLELISM}/${IDS}_${VARIATIONS}"
                    
                    #
                    # Run experiment for sorted dataset
                    #
                    SORTED_RESULTS_PATH="results/sorted/${GENERIC_RESULTS_PATH}"
                    SORTED_DATASET_PATH="../pit-joins-flink-thesis/datasets/unpartitioned/sorted/rows_per_cid_${KEYS_PER_ID}/${IDS}_${VARIATIONS}"
                    mkdir -p $SORTED_RESULTS_PATH

                    echo $SORTED_RESULTS_PATH
                    echo $SORTED_DATASET_PATH
                    # copy over jars
                    if [[ "$JOIN_TYPE" == "pit" ]]; then
                        echo "copying jars for pit-run"
                        cp ./unpartitioned_smj/flink-table-runtime-1.17.2.jar ../flink-1.17.2/lib/flink-table-runtime-1.17.2.jar
                        cp ./unpartitioned_smj/flink-table-planner_2.12-1.17.2.jar ../flink-1.17.2/opt/flink-table-planner_2.12-1.17.2.jar
                    fi


                    # start cluster
                    ../flink-1.17.2/bin/start-cluster.sh

                    mv "${SORTED_DATASET_PATH}/left" ./parquet_data/current
                    mv "${SORTED_DATASET_PATH}/right" ./parquet_data/current

                    # Run job
                    ../flink-1.17.2/bin/flink run ../pit-join-jar-builder/jars/unpartitioned/${JOIN_TYPE}_join.jar

                    # Reset directory path for current dataset
                    mv ./parquet_data/current/left $SORTED_DATASET_PATH
                    mv ./parquet_data/current/right $SORTED_DATASET_PATH

                    # collect all data
                    curl localhost:8081/jobs/overview > "${SORTED_RESULTS_PATH}/overview.json"
                    for i in $(jq '.jobs[] | .jid' "${SORTED_RESULTS_PATH}/overview.json"); do
                        temp="${i%\"}"
                        temp="${temp#\"}"
                        curl "localhost:8081/jobs/${temp}" > "${SORTED_RESULTS_PATH}/${temp}.json"
                    done

                    # stop cluster
                    ../flink-1.17.2/bin/stop-cluster.sh

                    #
                    # Run experiment for unsorted dataset
                    #
                    UNSORTED_RESULTS_PATH="results/unsorted/${GENERIC_RESULTS_PATH}"
                    UNSORTED_DATASET_PATH="../pit-joins-flink-thesis/datasets/unpartitioned/unsorted/rows_per_cid_${KEYS_PER_ID}/${IDS}_${VARIATIONS}"
                    mkdir -p $UNSORTED_RESULTS_PATH

                    if [[ "$JOIN_TYPE" == "pit" ]]; then
                        echo "copying jars for pit-run"
                        cp ./unpartitioned_smj/flink-table-runtime-1.17.2.jar ../flink-1.17.2/lib/flink-table-runtime-1.17.2.jar
                        cp ./unpartitioned_smj/flink-table-planner_2.12-1.17.2.jar ../flink-1.17.2/opt/flink-table-planner_2.12-1.17.2.jar
                    fi


                    # start cluster
                    ../flink-1.17.2/bin/start-cluster.sh

                    # Setup directory path for current dataset
                    mkdir -p ../pit-joins-flink-thesis/parquet_data/current

                    mv "${UNSORTED_DATASET_PATH}/left" ./parquet_data/current
                    mv "${UNSORTED_DATASET_PATH}/right" ./parquet_data/current

                    # Run job
                    ../flink-1.17.2/bin/flink run ../pit-join-jar-builder/jars/unpartitioned/${JOIN_TYPE}_join.jar

                    # Reset directory path for current dataset
                    mv ./parquet_data/current/left $UNSORTED_DATASET_PATH
                    mv ./parquet_data/current/right $UNSORTED_DATASET_PATH

                    # collect all data
                    curl localhost:8081/jobs/overview > "${UNSORTED_RESULTS_PATH}/overview.json"
                    for i in $(jq '.jobs[] | .jid' "${UNSORTED_RESULTS_PATH}/overview.json"); do
                        temp="${i%\"}"
                        temp="${temp#\"}"
                        curl "localhost:8081/jobs/${temp}" > "${UNSORTED_RESULTS_PATH}/${temp}.json"
                    done

                    # stop cluster
                    ../flink-1.17.2/bin/stop-cluster.sh

                    #
                    # Run experiment for partitioned dataset
                    #
                    PARTITIONED_RESULTS_PATH="results/partitioned/${GENERIC_RESULTS_PATH}"
                    PARTITIONED_DATASET_PATH="../pit-joins-flink-thesis/datasets/partitioned/sorted/rows_per_cid_${KEYS_PER_ID}/${IDS}_${VARIATIONS}"
                    mkdir -p $PARTITIONED_RESULTS_PATH

                    if [[ "$JOIN_TYPE" == "pit" ]]; then
                        echo "copying jars for pit-run"
                        cp ./partitioned_smj/flink-table-runtime-1.17.2.jar ../flink-1.17.2/lib/flink-table-runtime-1.17.2.jar
                        cp ./partitioned_smj/flink-table-planner_2.12-1.17.2.jar ../flink-1.17.2/opt/flink-table-planner_2.12-1.17.2.jar
                    fi


                    # start cluster
                    ../flink-1.17.2/bin/start-cluster.sh

                    # Setup directory path for current dataset
                    mkdir -p ../pit-joins-flink-thesis/parquet_data/current

                    mv "${PARTITIONED_DATASET_PATH}/left" ./parquet_data/current
                    mv "${PARTITIONED_DATASET_PATH}/right" ./parquet_data/current

                    # Run job
                    ../flink-1.17.2/bin/flink run ../pit-join-jar-builder/jars/partitioned/${JOIN_TYPE}_join.jar

                    # Reset directory path for current dataset
                    mv ./parquet_data/current/left $PARTITIONED_DATASET_PATH
                    mv ./parquet_data/current/right $PARTITIONED_DATASET_PATH

                    # collect all data
                    curl localhost:8081/jobs/overview > "${PARTITIONED_RESULTS_PATH}/overview.json"
                    for i in $(jq '.jobs[] | .jid' "${PARTITIONED_RESULTS_PATH}/overview.json"); do
                        temp="${i%\"}"
                        temp="${temp#\"}"
                        curl "localhost:8081/jobs/${temp}" > "${PARTITIONED_RESULTS_PATH}/${temp}.json"
                    done

                    # stop cluster
                    ../flink-1.17.2/bin/stop-cluster.sh
                done
            done
        done
        # sed -E -i '' "s/parallelism.default: [[:digit:]]/parallelism.default: 1/g" ../flink-1.17.2/conf/flink-conf.yaml
    done
done