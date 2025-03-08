# For all join types
    # For all parallelisms
        # For all ids
            # For all variations
                # Run unpartitioned sorted
                # Run unpartitioned unsorted
                # Run partitioned sorted

CONFIG="config.json"

echo "join_type,keys_per_id,parallelism,ids,variations,dataset_property,duration,fails"

jq -c '.join_types[]' $CONFIG | while read JOIN_TYPE; do
    temp="${JOIN_TYPE%\"}"
    temp="${temp#\"}"
    JOIN_TYPE=$temp
    jq -c '.keys_per_id_left[]' $CONFIG | while read KEYS_PER_ID; do
        jq -c '.parallelism[]' $CONFIG | while read PARALLELISM; do            
            jq -c '.ids[]' $CONFIG | while read IDS; do
                jq -c '.variations[]' $CONFIG | while read VARIATIONS; do
                    temp="${VARIATIONS%\"}"
                    temp="${temp#\"}"
                    VARIATIONS=$temp
                    GENERIC_RESULTS_PATH="${JOIN_TYPE}/keys_per_id_${KEYS_PER_ID}/parallelism_${PARALLELISM}/${IDS}_${VARIATIONS}"
                    SORTED_RESULTS_PATH="results/sorted/${GENERIC_RESULTS_PATH}"

                    DURATION="$(jq '.jobs[] | ."end-time" - ."start-time"' "${SORTED_RESULTS_PATH}/overview.json")"
                    FAILS="$(jq '.jobs[] | .tasks.total - .tasks.finished' "${SORTED_RESULTS_PATH}/overview.json")"
                    
                    echo "${JOIN_TYPE},${KEYS_PER_ID},${PARALLELISM},${IDS},${VARIATIONS},sorted,${DURATION},${FAILS}"
                    
                    
                    # echo $SORTED_RESULTS_PATH
                    # UNSORTED_RESULTS_PATH="results/unsorted/${GENERIC_RESULTS_PATH}"
                    # echo $UNSORTED_RESULTS_PATH
                    # PARTITIONED_RESULTS_PATH="results/partitioned/${GENERIC_RESULTS_PATH}"
                    # echo $PARTITIONED_RESULTS_PATH
                done
            done
        done
    done
done