cd /home/ec2-user/ActiveData-ETL/
export PYTHONPATH=.:vendor
git checkout etl
git pull origin etl

python2 activedata_etl/etl.py --settings=resources/settings/staging/etl.json
python2 activedata_etl/etl.py --settings=resources/settings/staging/etl.json  --key=tc.624745
#disown -h
#tail -f  results/logs/etl.log

# DO NOT HANG ONTO PROCESS (nohup)
# nohup python27 activedata_etl/etl.py --settings=etl_settings.json &
