#!/usr/bin/env bash

sudo yum install monit -y


# SIMPLE PLACE FOR LOGS
mkdir ~/logs
mkdir ~/logs/monit_emails
cd /
sudo ln -s /home/ec2-user/logs logs

cp ~/TestLog_ETL/resources/settings/monit.conf /etc/monit.conf


# CRON JOBS
sudo rm /var/spool/cron/ec2-user
sudo cp /home/ec2-user/TestLog-ETL/resources/cron/cronfile.txt /var/spool/cron/ec2-user

