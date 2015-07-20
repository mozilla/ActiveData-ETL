cd /home/ec2-user/TestLog-ETL/

git pull origin pulse-logger
export PYTHONPATH=.

python27 testlog_etl/pulse_logger.py --settings=resources/settings/staging/pulse_logger.json


tail -f  ~/TestLog-ETL/results/logs/pulse_logger.log

# DO NOT HANG ONTO PROCESS (nohup)
nohup python27 testlog_etl/pulse_logger.py --settings=resources/settings/staging/pulse_logger.json &
disown -h
tail -f  ~/TestLog-ETL/results/logs/pulse_logger.log

