
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

# PUT A COPY OF jre-8u131-linux-x64.rpm IN /home/ec2-user/
cd /home/ec2-user/
sudo rpm -i jre-8u131-linux-x64.rpm
sudo alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000
export JAVA_HOME=/usr/java/default

#CHECK IT IS 1.8
java -version

# INSTALL ELASTICSEARCH
cd /home/ec2-user/
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.0.0.tar.gz
tar zxfv elasticsearch-6.0.0.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-6.0.0/* /usr/local/elasticsearch/



# BE SURE TO MATCH THE PULGIN WITH ES VERSION
# https://github.com/elasticsearch/elasticsearch-cloud-aws
cd /usr/local/elasticsearch/
sudo bin/elasticsearch-plugin install discovery-ec2


# ES HEAD IS WONDERFUL!
# BE SURE YOUR elasticsearch.yml FILE IS HAS
#     http.cors.enabled: true
#     http.cors.allow-origin: "*"
cd /usr/local/elasticsearch/
sudo bin/elasticsearch-plugin install mobz/elasticsearch-head


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
sudo mkdir /data1

# ADD TO /etc/fstab SO AROUND AFTER REBOOT
sudo sed -i '$ a\/dev/xvdb   /data1       ext4    defaults,nofail  0   2' /etc/fstab


# TEST IT IS WORKING
sudo mount -a
sudo mkdir /data1/logs
sudo mkdir /data1/heapdump
sudo chown -R ec2-user:ec2-user /data1

# INCREASE THE FILE HANDLE LIMITS
# MUST USE nano TO REMOVE "unknown key"
sudo sed -i '$ a\fs.file-max = 100000' /etc/sysctl.conf
sudo sysctl -p

# INCREASE FILE HANDLE PERMISSIONS
sudo sed -i '$ a\ec2-user soft nofile 50000' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user hard nofile 100000' /etc/security/limits.conf

# INCREASE MEMORY PERMISSIONS
sudo sed -i '$ a\ec2-user soft memlock unlimited' /etc/security/limits.conf
sudo sed -i '$ a\ec2-user hard memlock unlimited' /etc/security/limits.conf

# EFFECTIVE LOGIN TO LOAD CHANGES TO FILE HANDLES
sudo -i -u ec2-user

# SHOW RESULTS
# prlimit

#INSTALL GIT
sudo yum install -y git-core

#CLONE THE primary BRANCH
cd ~
rm -fr ~/ActiveData-ETL
git clone https://github.com/klahnakoski/ActiveData-ETL.git
cd ~/ActiveData-ETL
git checkout push-to-es5

# COPY CONFIG FILES TO ES DIR
cd ~/ActiveData-ETL/
git pull origin push-to-es5
sudo cp ~/ActiveData-ETL/resources/elasticsearch/elasticsearch5_1.yml /usr/local/elasticsearch/config/elasticsearch.yml
sudo cp ~/ActiveData-ETL/resources/elasticsearch/jvm.options /usr/local/elasticsearch/config/jvm.options
sudo cp ~/ActiveData-ETL/resources/elasticsearch/log4j2.properties /usr/local/elasticsearch/config/log4j2.properties
sudo chown -R ec2-user:ec2-user /usr/local/elasticsearch


#INSTALL PYTHON27
sudo yum -y install python27
sudo pip install --upgrade pip
#rm -fr /home/ec2-user/temp
#mkdir  /home/ec2-user/temp
#cd /home/ec2-user/temp
#wget https://bootstrap.pypa.io/get-pip.py
#sudo python27 get-pip.py
#sudo ln -s /usr/local/bin/pip /usr/bin/pip

#INSTALL MODIFIED SUPERVISOR
sudo yum install -y libffi-devel
sudo yum install -y openssl-devel
sudo yum groupinstall -y "Development tools"

sudo pip install pyopenssl
sudo pip install ndg-httpsclient
sudo pip install pyasn1
sudo pip install requests
sudo pip install fabric==1.10.2
sudo pip install supervisor-plus-cron

cd /usr/bin
sudo ln -s /usr/local/bin/supervisorctl supervisorctl

sudo cp ~/ActiveData-ETL/resources/elasticsearch/supervisord.conf /etc/supervisord.conf

#START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
sudo /usr/local/bin/supervisord -c /etc/supervisord.conf
sudo supervisorctl reread
sudo supervisorctl update



cd ~/ActiveData-ETL/
git pull origin push-to-es5
sudo cp ~/ActiveData-ETL/resources/elasticsearch/elasticsearch5_1.yml /usr/local/elasticsearch/config/elasticsearch.yml
sudo cp ~/ActiveData-ETL/resources/elasticsearch/jvm.options /usr/local/elasticsearch/config/jvm.options
sudo cp ~/ActiveData-ETL/resources/elasticsearch/log4j2.properties /usr/local/elasticsearch/config/log4j2.properties
sudo chown -R ec2-user:ec2-user /usr/local/elasticsearch
cd /usr/local/elasticsearch
bin/elasticsearch

