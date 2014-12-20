






pyLibrary.env.elasticsearch
===========================

This module handles the lifecylce of an Elasticsearch index in the context of
ETL.  You only need this module if you are creating and retiring indexes. You
do not need this module for simply searching; for that I suggest using the
rest API directly.






###Settings###

Both ```Cluster``` and ```Index``` objects accept the same settings dict,
selecting only the properties it requires.

	{
		"host" : "http://192.168.0.98",
		"port" : 9200,
		"index" : "b2g_tests",
		"type" : "test_results",
		"debug" : true,
		"limit_replicas" : true,
		"schema_file" : "./resources/schema/test_schema.json"
	},







Cluster
-------


Index
-----
