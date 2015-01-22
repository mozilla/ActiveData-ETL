
cd /home/ec2-user/
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.2.tar.gz
tar zxfv elasticsearch-1.4.2.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-1.4.2/* /usr/local//elasticsearch/
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

# ENSURE THIS RETURNS "data", WHICH INDICATES NO FILESYTEM EXISTS
#[ec2-user@ip-172-31-0-7 dev]$ sudo file -s /dev/xvdb
#/dev/xvdb: data

sudo mkfs -t ext4 /dev/xvdb
sudo mkdir /data

#ADD TO /etc/fstab SO AROUND AFTER REBOOT
sudo sed -i '$ a\/dev/xvdb   /data        ext4    defaults,nofail  0   2' /etc/fstab

#TEST IT IS WORKING
sudo mount -a

#MAKE THE CONFIG FILE
cd /home/ec2-user
cat >elasticsearch.yml
# PASTE SETTINGS FILE HERE
# CTRL-D WHEN DONE









#COPY CONFIG FILE TO ES DIR
cd /usr/local/elasticsearch
sudo cp /home/ec2-user/elasticsearch.yml config/elasticsearch.yml

export ES_MIN_MEM=8G
export ES_MAX_MEM=8G

# SHOW CURRENT PID FOR KILLING LATER
sudo bin/elasticsearch -p current_pid.txt

# RUN IN BACKGROUND
bin/elasticsearch -d -p current_pid.txt
disown -h
