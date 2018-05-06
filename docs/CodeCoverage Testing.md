# CodeCoverage Testing

### Requires

* Elasticsearch 1.7 running locally on port 9200
* Python 2.7
* Always be sure to set your python path `export PYTHONPATH=.:vendor` (`set PYTHONPATH=.;vendor` on Windows)
* Use the [`etl` branch of the code](https://github.com/klahnakoski/ActiveData-ETL/tree/etl)

## ETL Overview

ETL process attaches its own metatdata to help track issues; these [can be seen in the `etl` property](https://activedata.allizom.org/tools/query.html#query_id=sziWmNiD).  For debugging, the most important is the `_id` (in elasticsearch), or `source_key` (in code), or just plain `key` (in command line args); which is a dot-delimited alpha-numeric sequence that represents the ETL stages.  [For any task (or coverage record), there is an `_id`]( https://activedata.allizom.org/tools/query.html#query_id=tANFCWV4), you will need this `_id` to run tests.  For example, Greg ran some coverage, and it has the `_id == tc.1530798:153078963.88`

## Coverage Overview

Coverage is [processed by `cov_to_es.py`](https://github.com/klahnakoski/ActiveData-ETL/blob/etl/activedata_etl/transforms/cov_to_es.py): It is responsible for switching between the types of coverage.  This switching is required because task are processed in blocks of 100, and there can be multiple coverage tasks in any one block, and each must be assigned a contiguous `_id`. Contiguous `_id`s allow me to find possible holes in the ETL.

### Artifact Testing
 
The `tests` directory has a number of broken "tests"; these all worked at some time in the past, but the tasks and artifacts they depend on have long expired, plus many rotted. This directory is a good source of code templates to make new tests.  Feel free to copy-and-paste a test, or change an existing one.

There will be three levels of testing. The first level is to test the artifact transformation; [`test_jsvm.py`](https://github.com/klahnakoski/ActiveData-ETL/blob/etl/tests/test_jsvm.py) is a good example of that: It downloads a known artifact and writes the result to a file.  I have not looked at the per-test coverage artifacts, you may need to expand this to cover multiple artifacts (?do per-test tasks generate multiple artifacts?). Running the test should be as simple as using `unittest` to run the test:
  
    python -m unittest discover -s tests -p test_jsvm.py`

### Integration Testing

The second level is to get the ETL framework involved.  This will test the `cov_to_es.py` switching code, and provide [legitimate parameters to the transformation method](https://github.com/klahnakoski/ActiveData-ETL/blob/etl/activedata_etl/transforms/__init__.py#L91).  This will require a config file to point to all the correct resources.  The `resources/settings/codecoverage` directory has an number of config files, one of which I just updated (but have not tested).  This step will make the full-and-proper artifacts and upload them to test bucket in S3.

    export PYTHONPATH=.:vendor
    python activedata_etl\etl.py --settings=resources/settings/codecoverage/etl.json  --key=tc.1538295

This second level requires access to AWS S3 buckets and test queues; credentials are hidden in `~/private.json` (which you can references to in `etl.json` file).  

### Scale Testing

The third level of "testing" is a merge into the `etl` branch:  The workers are scaled down; the changes are pushed to the remaining workers; and we watch for errors as we scale back up.  At this stage the corner cases will reveal themselves;  It is important that the code catches as much context as possible to minimize the work needed to find inevitable bugs; and it is good if it fails early: The ETL framework will catch all exceptions, report them via email, and return the work back to the queue to be tried again (with hopefully an improved ETL code).
