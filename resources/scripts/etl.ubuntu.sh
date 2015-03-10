cd /home/ubuntu/TestLog-ETL/
git checkout etl
git stash
git pull origin etl
git stash apply

export PYTHONPATH=.
python testlog_etl/etl.py --settings=resources/settings/etl_staging_settings.json
