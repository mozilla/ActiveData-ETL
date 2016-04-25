#!/usr/bin/env bash


sudo yum -y install python27
sudo yum -y install git
sudo yum install monit -y

#INSTALL PYTHON
mkdir  /home/ec2-user/temp
cd  /home/ec2-user/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python27 get-pip.py

#INSTALL BUILDBOT IMPORT
cd  /home/ec2-user
git clone https://github.com/klahnakoski/TestLog-ETL.git
cd /home/ec2-user/TestLog-ETL/
git checkout manager

#INSTALL SpotManager
cd  /home/ec2-user
git clone https://github.com/klahnakoski/SpotManager.git
cd /home/ec2-user/SpotManager/
git checkout manager




# SIMPLE PLACE FOR LOGS
mkdir ~/logs
mkdir ~/logs/monit_emails
cd /
sudo ln -s /home/ec2-user/logs logs

cp ~/TestLog_ETL/resources/settings/monit.conf /etc/monit.conf

# CRON JOBS
chmod u+x /home/ec2-user/TestLog-ETL/resources/scripts/run_buildbot_json_logs.sh
chmod u+x /home/ec2-user/SpotManager/examples/scripts/run_es.sh
chmod u+x /home/ec2-user/SpotManager/examples/scripts/run_etl.sh

sudo rm /var/spool/cron/ec2-user
sudo cp /home/ec2-user/TestLog-ETL/resources/cron/manager.cron /var/spool/cron/ec2-user



