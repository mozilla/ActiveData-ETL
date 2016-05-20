#!/usr/bin/env bash




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
git clone https://github.com/klahnakoski/TestLog-ETL.git
cd /home/ec2-user/TestLog-ETL/
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

# CRON JOBS
chmod u+x /home/ec2-user/TestLog-ETL/resources/scripts/run_buildbot_json_logs.sh
chmod u+x /home/ec2-user/SpotManager/examples/scripts/run_es.sh
chmod u+x /home/ec2-user/SpotManager/examples/scripts/run_etl.sh
chmod u+x /home/ec2-user/ActiveData/resources/scripts/run_codecoverage.sh

# CRON FILE (TURN "OFF" AND "ON", RESPECTIVLY)
sudo rm /var/spool/cron/ec2-user
sudo cp /home/ec2-user/TestLog-ETL/resources/cron/manager.cron /var/spool/cron/ec2-user



