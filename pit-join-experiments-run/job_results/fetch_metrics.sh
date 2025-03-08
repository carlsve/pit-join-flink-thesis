

for i in $(jq '.jobs[] | .jid' overview.json); do
    temp="${i%\"}"
    temp="${temp#\"}"
    curl "localhost:8081/jobs/${temp}" > "${temp}.json"
done