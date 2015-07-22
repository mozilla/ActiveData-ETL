cd /home/ubuntu/TestLog-ETL/
git checkout etl
git pull origin etl


# DO NOT HANG ONTO PROCESS (nohup)
export PYTHONPATH=.
export PYPY_GC_MAX=2GB
pypy testlog_etl/etl.py --settings=resources/settings/staging/etl.json &
disown -h
tail -f results/logs/etl.log
