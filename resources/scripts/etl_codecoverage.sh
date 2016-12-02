#!/bin/sh

export PYTHONPATH=.
python activedata_etl/etl.py --settings=resources/settings/codecoverage/etl.json --key=tc.420357
