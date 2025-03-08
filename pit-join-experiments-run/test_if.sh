CONFIG="config.json"

jq -c '.join_types[]' $CONFIG | while read JOIN_TYPE; do
    temp="${JOIN_TYPE%\"}"
    temp="${temp#\"}"
    JOIN_TYPE=$temp

    echo $JOIN_TYPE

    if [[ "$JOIN_TYPE" == "pit" ]]; then
        echo "weeeee"
    fi
done
