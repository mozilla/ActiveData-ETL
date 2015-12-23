The Many Tasks
==============

This ETL library performs many functions, and they are all listed in this 
directory.  Here are some, listed in most-important-first order:


Module `pulse_logger`
---------------------

A stand-alone program that stays connected to Mozilla's Pulse queue and 
archives the messages to S3, along with putting them on the work queue.  This 
program is the start of the ETL pipeline, and most important because Pulse
messages last only a couple of hours before they are lost (due to queue overflow).

Module `etl`
---------------

This contains the main routine responsible for using `transforms` and applying 
them against a queue of work to be done.  


Module `backfill`
-----------------

Given a set of conditions, this will review S3 and fill the work queue with 
items not found in ES

Module `push_to_es`
-------------------

Responsible for adding S3 records into ES, with little or no transform.

Module `update_etl`
-------------------

If the `etl` or `transform` code is changed, you can push those changes to the 
worker machines immediately.  All workers use the `etl` branch, so be sure 
the changes you want are there.
