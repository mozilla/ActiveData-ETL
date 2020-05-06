

# Development 

## Running tests

The tests allow you to work on an ETL transform without the need for read/write access to the overall system.
Unfortunately, the tests refer to URLs, and those URLs have likely expired by now. If you are debugging an ETL transform, then find the bests-matching test, update its URL to point to recent data, then start your work.

## Processing Artifacts Locally

Once you have access to the S3 buckets and SQS, you may let your development machine join the hoard of ETL workers.  

In general, local processing follows this procedure:

* Make local queue for a todo list, and fill it
* Pull work item off the queue
* Extract S3 file
* Transform data 
* Load result back to S3 (same name, but different bucket)

The easiest way to do this is to run the `etl.py` module on a block of data: Pointing to the `--config` file and select the `--key` you wish to process

    export PYTHONPATH=.:tests
    python activedata_etl\etl.py --config=resources\settings\dev_to_staging\etl.json --key=tc.2759878

If you successfully process an artifact, then the other machines will pick up the result and process it further, or insert it into the ES cluster.  You can inspect it with ActiveData.

If you do not select a `--key` then your local machine will connect to the central work queue to get items to work on; your machine will act just like any other worker.  

## Development with Production data?

Yes, development is done with production data. The data is always changing, so recent data is better. There is little need to be concerned about your development machine ruining data.
 
 * There are hundreds of ETL machines. Any negative impact will be minor compared to the total data volume. 
 * Even so, data is only ruined when your machine confirms the work item at the end of the transformation step, which hardly happens during your debug cycle
 * Finally, everything in the ETL pipeline is reversible, so any errors introduced by development errors can be over written with a backfill later.  

