sudo cp /home/ec2-user/elasticsearch.yml /usr/local/elasticsearch/config/elasticsearch.yml
sudo cp /home/ec2-user/elasticsearch.in.sh /usr/local/elasticsearch/bin/elasticsearch.in.sh

cd /usr/local/elasticsearch

# RUN IN BACKGROUND
sudo bin/elasticsearch -p current_pid.txt &
disown -h

curl -XPUT localhost:9200/_cluster/settings -d '{"transient":{"cluster.routing.allocation.disable_allocation": false}}'

tail -f /data1/logs/active-data.log

