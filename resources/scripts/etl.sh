cd /home/ec2-user/TestLog-ETL/
export PYTHONPATH=.

# DO NOT HANG ONTO PROCESS (nohup)
nohup python27 testlog_etl/etl.py --settings=etl_settings.json &

