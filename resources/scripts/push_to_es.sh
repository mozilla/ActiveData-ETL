cd /home/ec2-user/TestLog-ETL/

git pull origin push-to-es
export PYTHONPATH=.

python27 testlog_etl/push_to_es.py --settings=resources/settings/push_to_es_staging_settings.json &
disown -h


