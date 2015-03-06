
# FOR AMAZON AMI ONLY
# ENSURE THE EC@ INSTANCE IS GIVEN A ROLE THAT ALLOWS IT ACCESS TO S3 AND DISCOVERY
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

# ENSURE THE FOLLOWING FILES HAVE BEEN UPLOADED FIRST
# /home/ec2-user/elasticsearch.yml
# /home/ec2-user/elasticsearch.in.sh

# NOTE: NODE DISCOVERY WILL ONLY WORK IF PORT 9300 IS OPEN BETWEEN THEM


cd /home/ec2-user/
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.2.tar.gz
tar zxfv elasticsearch-1.4.2.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-1.4.2/* /usr/local/elasticsearch/
cd /usr/local/elasticsearch/

# BE SURE TO MATCH THE PUGLIN WITH ES VERSION
# https://github.com/elasticsearch/elasticsearch-cloud-aws

sudo bin/plugin -install elasticsearch/elasticsearch-cloud-aws/2.4.1


#ES HEAD IS WONDERFUL!
#http://54.69.134.49:9200/_plugin/head/

sudo bin/plugin -install mobz/elasticsearch-head


#MOUNT AND FORMAT THE EBS VOLUME

# EXAMPLE OF LISTING THE BLOCK DEV ICES
# [ec2-user@ip-172-31-0-7 dev]$ lsblk
# NAME    MAJ:MIN RM SIZE RO TYPE MOUNTPOINT
# xvda    202:0    0   8G  0 disk
# ââxvda1 202:1    0   8G  0 part /
# xvdb    202:16   0   1T  0 disk

# ENSURE THIS RETURNS "data", WHICH INDICATES NO FILESYSTEM EXISTS
#[ec2-user@ip-172-31-0-7 dev]$ sudo file -s /dev/xvdb
#/dev/xvdb: data

sudo mkfs -t ext4 /dev/xvdb
sudo mkdir /data
sudo mkdir /data/logs

#ADD TO /etc/fstab SO AROUND AFTER REBOOT
sudo sed -i '$ a\/dev/xvdb   /data        ext4    defaults,nofail  0   2' /etc/fstab

#TEST IT IS WORKING
sudo mount -a

# COPY CONFIG FILE TO ES DIR
sudo cp /home/ec2-user/elasticsearch.yml /usr/local/elasticsearch/config/elasticsearch.yml

# FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
# THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
sudo cp /home/ec2-user/elasticsearch.in.sh /usr/local/elasticsearch/bin/elasticsearch.in.sh

# RUN IN BACKGROUND
export ES_MIN_MEM=12g
export ES_MAX_MEM=12g
cd /usr/local/elasticsearch
sudo bin/elasticsearch -p current_pid.txt &
disown -h
cd /usr/local/elasticsearch


cd /usr/local/elasticsearch
tail -f logs/ekyle-aws-1.log
