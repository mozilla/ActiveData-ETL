cd /home/ec2-user/TestLog-ETL/


git pull origin push-to-es
export PYTHONPATH=.

nohup python27 testlog_etl/push_unit_to_es.py --settings=resources/settings/staging/push_unit_to_es.json >& /dev/null < /dev/null &


