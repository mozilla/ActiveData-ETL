set PYTHONPATH=.
set PYPY_GC_MAX=3GB
pypy activedata_etl/etl.py --settings=resources\settings\dev/etl.json
