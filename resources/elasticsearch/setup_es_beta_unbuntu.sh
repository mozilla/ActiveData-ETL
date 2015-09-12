# ENSURE THE FOLLOWING FILES HAVE BEEN UPLOADED FIRST
# /home/klahnakoski/elasticsearch_beta.yml
# /home/klahnakoski/elasticsearch_beta.in.sh

# NOTE: NODE DISCOVERY WILL ONLY WORK IF PORT 9300 IS OPEN BETWEEN THEM

# ORACLE'S JAVA VERISON 8 IS APPARENTLY MUCH FASTER
# YOU MUST AGREE TO ORACLE'S LICENSE TERMS TO USE THIS COMMAND
cd /home/klahnakoski/
mkdir temp
cd temp
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get install oracle-java8-installer

#CHECK IT IS 1.8
java -version

cd /home/klahnakoski/
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.5.2.tar.gz
tar zxfv elasticsearch-1.5.2.tar.gz
sudo mkdir -p /usr/local/elasticsearch
sudo cp -R elasticsearch-1.5.2/* /usr/local/elasticsearch/
cd /usr/local/elasticsearch/

ES HEAD IS WONDERFUL!
#http://54.69.134.49:9200/_plugin/head/
sudo bin/plugin -install mobz/elasticsearch-head

#INSTALL BIGDESK
sudo bin/plugin -install lukas-vlcek/bigdesk

# COPY CONFIG FILE TO ES DIR
sudo cp /home/klahnakoski/elasticsearch_beta.yml /usr/local/elasticsearch/config/elasticsearch.yml

# FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
# THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
sudo cp /home/klahnakoski/elasticsearch_beta.in.sh /usr/local/elasticsearch/bin/elasticsearch.in.sh

# RUN IN BACKGROUND
export ES_MIN_MEM=5g
export ES_MAX_MEM=5g
cd /usr/local/elasticsearch
sudo bin/elasticsearch -p current_pid.txt &
disown -h


