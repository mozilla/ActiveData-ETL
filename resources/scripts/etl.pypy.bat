set PYTHONPATH=.
set PYPY_GC_MAX=3GB
pypy testlog_etl/etl.py --settings=resources\settings\dev/etl.json
