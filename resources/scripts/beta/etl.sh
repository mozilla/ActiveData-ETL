


cd ~/ActiveData-ETL/
git checkout beta
sudo pip install -r requirements.txt


cd ~/ActiveData-ETL/
git pull origin beta
export PYTHONPATH=.

python activedata_etl/pulse_logger.py --settings=resources/settings/beta/pulse_logger.json  >& /dev/null < /dev/null &
python activedata_etl/etl.py --settings=resources/settings/beta/etl.json >& /dev/null < /dev/null &
python activedata_etl/push_to_es.py --settings=resources/settings/beta/push_unit_to_es.json >& /dev/null < /dev/null &
python activedata_etl/push_to_es.py --settings=resources/settings/beta/push_jobs_to_es.json >& /dev/null < /dev/null &
python activedata_etl/push_to_es.py --settings=resources/settings/beta/push_perf_to_es.json >& /dev/null < /dev/null &

disown -h
