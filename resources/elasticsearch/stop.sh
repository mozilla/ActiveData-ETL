# ENSURE SHARDS DO NOT MOVE AROUND DURING RESTART
curl -XPUT localhost:9200/_cluster/settings -d '{"transient":{"cluster.routing.allocation.disable_allocation": true}}
