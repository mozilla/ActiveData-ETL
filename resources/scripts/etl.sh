cd /home/ubuntu/TestLog-ETL/
export PYTHONPATH=.
git checkout etl
git pull origin etl

python2 testlog_etl/etl.py --settings=resources/settings/etl_staging_settings.json &
disown -h
tail -f  results/logs/etl.log

# DO NOT HANG ONTO PROCESS (nohup)
#nohup python27 testlog_etl/etl.py --settings=etl_settings.json &
