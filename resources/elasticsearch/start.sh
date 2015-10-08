cd /usr/local/elasticsearch

# RUN IN BACKGROUND
sudo bin/elasticsearch -p current_pid.txt &
disown -h

curl -XPUT localhost:9200/_cluster/settings -d '{"transient":{"cluster.routing.allocation.disable_allocation": false}}'

tail -f /data1/logs/active-data.log

curl -XPOST 'localhost:9200/_cluster/reroute' -d '{"commands": [{ "allocate": { "index": "saved_queries", "shard": 1, "node": "secondary3", "allow_primary": 1 } }]}'
