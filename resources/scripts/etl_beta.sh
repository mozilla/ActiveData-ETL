


cd ~/TestLog-ETL/
git checkout beta
sudo pip install -r requirements.txt


cd ~/TestLog-ETL/
git pull origin beta
export PYTHONPATH=.

python testlog_etl/pulse_logger.py --settings=resources/settings/pulse_logger_beta_settings.json  >& /dev/null < /dev/null &
python testlog_etl/etl.py --settings=resources/settings/beta/etl.json >& /dev/null < /dev/null &
python testlog_etl/push_unit_to_es.py --settings=resources/settings/push_unit_to_es_beta_settings.json >& /dev/null < /dev/null &

disown -h
