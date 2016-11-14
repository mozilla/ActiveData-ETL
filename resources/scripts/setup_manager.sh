#!/usr/bin/env bash

sudo ifconfig eth0 mtu 1500


sudo yum -y install git

# INSTALL PYTHON
sudo yum -y install python27
sudo easy_install pip

# BUILDBOT PROCESSING REQUIRES THE FOLLOWING POLICY
#{
#    "Version": "2012-10-17",
#    "Statement": [
#        {
#            "Effect": "Allow",
#            "Action": [
#                "sqs:*",
#                "s3:*"
#            ],
#            "Resource": [
#                "*"
#            ]
#        }
#    ]
#}
# INSTALL BUILDBOT IMPORT
cd  /home/ec2-user
git clone https://github.com/klahnakoski/ActiveData-ETL.git
cd /home/ec2-user/ActiveData-ETL/
git checkout manager

sudo /usr/local/bin/pip install -r requirements.txt

# SPOT MANAGER REQUIRES THE FOLLOWING POLICY
#{
#    "Version": "2012-10-17",
#    "Statement": [
#        {
#            "Effect": "Allow",
#            "Action": [
#                "ec2:*",
#                "ses:*",
#                "sqs:*",
#                "s3:*",
#                "iam:PassRole"
#            ],
#            "Resource": [
#                "*"
#            ]
#        }
#    ]
#}
# INSTALL SpotManager
cd  /home/ec2-user
git clone https://github.com/klahnakoski/SpotManager.git
cd /home/ec2-user/SpotManager/
git checkout manager
sudo /usr/local/bin/pip install -r requirements.txt

# INSTALL ActiveData
cd  /home/ec2-user
git clone https://github.com/klahnakoski/ActiveData.git
cd /home/ec2-user/ActiveData/
git checkout manager
sudo /usr/local/bin/pip install -r requirements.txt

# INSTALL TestFailures
cd  /home/ec2-user
git clone https://github.com/klahnakoski/TestFailures.git
cd /home/ec2-user/TestFailures/
git checkout manager

# INSTALL MoDataSubmission
# BE SURE TO INCLUDE STORAGE PERMISSIONS FILE
# put ~/storage_permissions.json ~/storage_permissions.json
chmod 600 ~/storage_permissions.json

cd ~
git clone https://github.com/klahnakoski/MoDataSubmission.git
cd /home/ec2-user/MoDataSubmission
git checkout master
git pull origin master
sudo pip install -r requirements.txt
sudo -i
export PYTHONPATH=.
export HOME=/home/ec2-user
cd ~/MoDataSubmission
nohup python27 modatasubmission/app.py --settings=resources/config/prod.json &
disown -h
exit

# SIMPLE PLACE FOR LOGS
mkdir ~/logs
mkdir ~/logs/monit_emails
cd /
sudo ln -s /home/ec2-user/logs logs

# COPY KEYS TO MACHINE
#put ~/private_active_data_etl.json ~/private_active_data_etl.json
cp ~/private_active_data_etl.json ~/private.json
chmod 600 ~/private_active_data_etl.json
chmod 600 ~/private.json

#put ~/.ssh/aws-pulse-logger.pem ~/.ssh/aws-pulse-logger.pem
chmod 600 ~/.ssh/aws-pulse-logger.pem


# INSTALL esShardBalancer
cd ~
git clone https://github.com/klahnakoski/esShardBalancer.git
cd ~/esShardBalancer
git checkout master
# RUN IT
chmod u+x /home/ec2-user/esShardBalancer/resources/scripts/staging/balance.sh
/home/ec2-user/esShardBalancer/resources/scripts/staging/balance.sh



# CRON JOBS
chmod u+x /home/ec2-user/ActiveData-ETL/resources/scripts/run_buildbot_json_logs.sh
chmod u+x /home/ec2-user/SpotManager/examples/scripts/run_es.sh
chmod u+x /home/ec2-user/SpotManager/examples/scripts/run_etl.sh
chmod u+x /home/ec2-user/ActiveData/resources/scripts/run_codecoverage.sh
chmod u+x /home/ec2-user/TestFailures/resources/scripts/agg_job.sh

# CRON FILE (TURN "OFF" AND "ON", RESPECTIVLY)
sudo rm /var/spool/cron/ec2-user
sudo cp /home/ec2-user/ActiveData-ETL/resources/cron/manager.cron /var/spool/cron/ec2-user



