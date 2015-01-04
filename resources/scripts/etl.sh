cd /home/ec2-user/TestLog-ETL/
export PYTHONPATH=.
git checkout etl
git pull origin etl

# DO NOT HANG ONTO PROCESS (nohup)
nohup python27 testlog_etl/etl.py --settings=etl_settings.json &

