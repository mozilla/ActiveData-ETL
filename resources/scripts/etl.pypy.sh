cd /home/ubuntu/temp/TestLog-ETL/
export PYTHONPATH=.

# DO NOT HANG ONTO PROCESS (nohup)
pypy testlog_etl/etl.py --settings=etl_settings.json &

