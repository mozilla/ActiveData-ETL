cd /home/ec2-user/TestLog-ETL/
export PYTHONPATH=.

# DO NOT HANG ONTO PROCESS (nohup)
nohup python27 testlog_etl/pulse_logger.py --settings=pulse_logger_settings.json &

