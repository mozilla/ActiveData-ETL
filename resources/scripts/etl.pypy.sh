cd /home/ubuntu/TestLog-ETL/
git checkout etl
git pull origin etl


# DO NOT HANG ONTO PROCESS (nohup)
export PYTHONPATH=.
export PYPY_GC_MAX=6GB
pypy testlog_etl/etl.py --settings=resources/settings/etl_staging_settings.json &
disown -h
tail -f results/logs/etl.log
