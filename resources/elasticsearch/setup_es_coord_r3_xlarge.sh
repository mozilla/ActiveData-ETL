
# FOR AMAZON AMI ONLY
# ENSURE THE EC2 INSTANCE IS GIVEN A ROLE THAT ALLOWS IT ACCESS TO S3 AND DISCOVERY
# THIS EXAMPLE WORKS, BUT YOU MAY FIND IT TOO PERMISSIVE
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "NotAction": "iam:*",
#       "Resource": "*"
#     }
#   ]
# }


# NOTE: NODE DISCOVERY WILL ONLY WORK IF PORT 9300 IS OPEN BETWEEN THEM

# ORACLE'S JAVA VERISON 8 IS APPARENTLY MUCH FASTER
# YOU MUST AGREE TO ORACLE'S LICENSE TERMS TO USE THIS COMMAND
cd /home/ec2-user/
mkdir temp
cd temp
wget -c --no-cookies --no-check-certificate --header "Cookie: s_cc=true; s_nr=1425654197863; s_sq=%5B%5BB%5D%5D; oraclelicense=accept-securebackup-cookie; gpw_e24=http%3A%2F%2Fwww.oracle.com%2Ftechnetwork%2Fjava%2Fjavase%2Fdownloads%2Fjre8-downloads-2133155.html" "http://download.oracle.com/otn-pub/java/jdk/8u40-b25/jre-8u40-linux-x64.rpm" --output-document="jdk-8u5-linux-x64.rpm"
sudo rpm -i jdk-8u5-linux-x64.rpm
sudo alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000
export JAVA_HOME=/usr/java/default

#CHECK IT IS 1.8
java -version

cd /home/ec2-user/
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.2.tar.gz
tar zxfv elasticsearch-1.4.2.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-1.4.2/* /usr/local/elasticsearch/
cd /usr/local/elasticsearch/

# BE SURE TO MATCH THE PULGIN WITH ES VERSION
# https://github.com/elasticsearch/elasticsearch-cloud-aws

sudo bin/plugin -install elasticsearch/elasticsearch-cloud-aws/2.4.1


#ES HEAD IS WONDERFUL!
#http://54.69.134.49:9200/_plugin/head/

sudo bin/plugin -install mobz/elasticsearch-head





# COPY CONFIG FILE TO ES DIR
sudo cp /home/ec2-user/TestLog-ETL/resources/elasticsearch/elasticsearch_coord.yml /usr/local/elasticsearch/config/elasticsearch.yml

# FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
# THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
sudo cp /home/ec2-user/TestLog-ETL/resources/elasticsearch/elasticsearch.in.sh /usr/local/elasticsearch/bin/elasticsearch.in.sh


# RUN IN BACKGROUND

cd /usr/local/elasticsearch
sudo bin/elasticsearch -p current_pid.txt &
disown -h

tail -f /data/logs/ekyle-aws-1.log



