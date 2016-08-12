cd /home/ec2-user/Activedata-ETL/

git pull origin pulse-logger
export PYTHONPATH=.

python27 activedata_etl/pulse_logger.py --settings=resources/settings/staging/pulse_logger.json


tail -f  ~/Activedata-ETL/results/logs/pulse_logger.log

# DO NOT HANG ONTO PROCESS (nohup)
export PYTHONPATH=.
nohup python27 activedata_etl/pulse_logger.py --settings=resources/settings/staging/pulse_logger.json &
disown -h
tail -f  ~/Activedata-ETL/results/logs/pulse_logger.log

