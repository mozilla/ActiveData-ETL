#!/usr/bin/env bash

cd /home/ec2-user/TestLog-ETL
export PYTHONPATH=.
python testlog_etl/buildbot_json_jogs.py  --settings resources/settings/staging/buildbot_json_logs.json
