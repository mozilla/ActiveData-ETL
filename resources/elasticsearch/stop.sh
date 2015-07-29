# ENSURE SHARDS DO NOT MOVE AROUND DURING RESTART
curl -XPUT localhost:9200/_cluster/settings -d '{"transient":{"cluster.routing.allocation.disable_allocation": true}}'

curl -XPOST http://localhost:9200/_cluster/nodes/_local/_shutdown
tail -f /data1/logs/active-data.log


curl -XPOST http://localhost:9200/_shutdown


tail -f /data1/logs/active-data.log
