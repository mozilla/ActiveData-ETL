cd /home/ubuntu/Activedata-ETL/
git checkout etl
git stash
git pull origin etl
git stash apply

export PYTHONPATH=.
python activedata_etl/etl.py --settings=resources/settings/staging/etl.json


