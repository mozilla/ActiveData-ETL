#!/usr/bin/env bash

cd /home/ec2-user/ActiveData-ETL
export PYTHONPATH=.
python activedata_etl/buildbot_json_logs.py  --settings resources/settings/staging/buildbot_json_logs.json
