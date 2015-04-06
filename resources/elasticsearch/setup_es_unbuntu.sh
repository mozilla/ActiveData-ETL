
# FOR UBUNTU MACHINE

# ENSURE THE FOLLOWING FILES HAVE BEEN UPLOADED FIRST
# ~/elasticsearch.yml
# ~/elasticsearch.in.sh

# NOTE: NODE DISCOVERY WILL ONLY WORK IF PORT 9300 IS OPEN BETWEEN THEM

# ORACLE'S JAVA VERISON 8 IS APPARENTLY MUCH FASTER
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer

#CHECK IT IS 1.8
java -version

cd ~/
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.5.0.tar.gz
tar zxfv elasticsearch-1.5.0.tar.gz


#ES HEAD IS WONDERFUL!
#http://54.69.134.49:9200/_plugin/head/

cd ~/elasticsearch-1.5.0/
sudo bin/plugin -install mobz/elasticsearch-head

# RUN IN BACKGROUND
export ES_MIN_MEM=12g
export ES_MAX_MEM=12g
cd ~/elasticsearch-1.5.0
sudo bin/elasticsearch -p current_pid.txt &
disown -h

cd ~/elasticsearch-1.5.0/
tail -f logs/ekyle-aws-1.log
