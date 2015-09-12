
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

# INSTALL ELASTICSEARCH
cd /home/ec2-user/
wget https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.1.tar.gz
tar zxfv elasticsearch-1.7.1.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-1.7.1/* /usr/local/elasticsearch/


cd /usr/local/elasticsearch/

# BE SURE TO MATCH THE PULGIN WITH ES VERSION
# https://github.com/elasticsearch/elasticsearch-cloud-aws
sudo bin/plugin install elasticsearch/elasticsearch-cloud-aws/2.7.1

#ES HEAD IS WONDERFUL!
sudo bin/plugin install mobz/elasticsearch-head


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

#FORMAT AND MOUNT
sudo mkfs -t ext4 /dev/xvdb
sudo mkfs -t ext4 /dev/xvdc
sudo mkfs -t ext4 /dev/xvdd
sudo mkfs -t ext4 /dev/xvde

#MOUNT (NO FORMAT)
sudo mount /dev/xvdb /data1
sudo mount /dev/xvdb /data1


sudo mkdir /data1
sudo mkdir /data2
sudo mkdir /data3

# ADD TO /etc/fstab SO AROUND AFTER REBOOT
sudo sed -i '$ a\/dev/xvdc   /data1       ext4    defaults,nofail  0   2' /etc/fstab
sudo sed -i '$ a\/dev/xvdd   /data2       ext4    defaults,nofail  0   2' /etc/fstab
sudo sed -i '$ a\/dev/xvde   /data3       ext4    defaults,nofail  0   2' /etc/fstab

# TEST IT IS WORKING
sudo mount -a
sudo mkdir /data1/logs
sudo mkdir /data1/heapdump

# INCREASE THE FILE HANDLE LIMITS
sudo sed -i '$ a\fs.file-max = 100000' /etc/sysctl.conf
sudo sysctl -p

# INCREASE FILE HANDLE PERMISSIONS
sudo sed -i '$ a\ec2-user soft nofile 50000' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user hard nofile 100000' /etc/security/limits.conf

# EFFECTIVE LOGIN TO LOAD CHANGES TO FILE HANDLES
sudo -i -u ec2-user

# SHOW RESULTS
# prlimit

#INSTALL GIT
sudo yum install -y git-core

#CLONE THE primary BRANCH
cd ~
rm -fr ~/TestLog-ETL
git clone https://github.com/klahnakoski/TestLog-ETL.git
cd ~/TestLog-ETL
git checkout primary

# COPY CONFIG FILE TO ES DIR
sudo cp ~/TestLog-ETL/resources/elasticsearch/elasticsearch_primary.yml /usr/local/elasticsearch/config/elasticsearch.yml

# FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
# THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
sudo cp ~/TestLog-ETL/resources/elasticsearch/elasticsearch.in.sh /usr/local/elasticsearch/bin/elasticsearch.in.sh


#INSTALL PYTHON27
sudo yum -y install python27

rm -fr /home/ec2-user/temp
mkdir  /home/ec2-user/temp
cd /home/ec2-user/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python27 get-pip.py
sudo ln -s /usr/local/bin/pip /usr/bin/pip

#INSTALL MODIFIED SUPERVISOR
sudo yum install -y libffi-devel
sudo yum install -y openssl-devel
sudo yum groupinstall -y "Development tools"

sudo pip install pyopenssl
sudo pip install ndg-httpsclient
sudo pip install pyasn1
sudo pip install requests
sudo pip install supervisor-plus-cron

cd /usr/bin
sudo ln -s /usr/local/bin/supervisorctl supervisorctl

sudo cp ~/TestLog-ETL/resources/elasticsearch/supervisord.conf /etc/supervisord.conf

#START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
sudo /usr/local/bin/supervisord -c /etc/supervisord.conf
sudo /usr/local/bin/supervisorctl reread
sudo /usr/local/bin/supervisorctl update
