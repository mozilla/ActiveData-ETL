ActiveData-ETL
===========

The ETL code responsible for filling ActiveData.

Sounds Exciting!  Can I Use This?
---------------------------------

Probably not.  The majority of the code implements a high volume idiosyncratic
data pipeline on top of AWS services, and requires other services to work in
tandem with this.  But, feel free to pillage `activedata_etl/imports` or
`activedata_etl/transforms` for the transformation code.


Branches
--------

Many branches are meant as stable versions for each of the processes involved
in the ETL.  Ideally, they would be unified, but library upgrades can cause
unique instability: deployment of a branch does not happen until (manual)
testing has been done.

Here are the important branches:

* **dev** - unstable - primary branch for accepting changes
* **etl** - stable - for ETL machines
* **primary** - stable - for the "primary" and "coordinator" ES nodes
* **codecoverage** - unstable - for Code Coverage ETL development
* **pulse-logger** - stable - for the PulseLogger
* **tc-logger** - stable - for the TaskCluster logger
* **push-to-es** - stable - code installed on ES spot instance machines for
final indexing.
* **beta** - stable - of all branches for testing on the beta machines
* **manager** - stable - installed on the ActiveData management machine for cron jobs
* **master** - unstable - intermittently updated to track **dev**, eventually
intended as the single-stable-version


Requirements
------------

* Python 2.7.x
* [Elasticsearch 1.7.x](https://www.elastic.co/downloads/past-releases/elasticsearch-1-7-5)  (the current 2.x versions are not supported yet)
* Access to Amazon S3 bucket for ETL results
* Access to Amazon SQS for the ETL pipeline


Installing Fabric
-----------------

It is 2016, and Python is still hard on Windows.  It would be [a nice question for Stack Overflow](http://stackoverflow.com/questions/9000380/install-python-fabric-on-windows), but apparently not.

1. [Install Python, and PIP](https://github.com/klahnakoski/pyLibrary#windows-7-install-instructions-for-python)
2. `pip install fabric` - There will be errors
3. Install pycrypto.  Hopefully, [voidspace](http://www.voidspace.org.uk/python/modules.shtml) still provides pre-compiled binaries.  Knowing the internet, it probably moved by the time you read this, so I made a [copy of pycrypto-2.6.win32-py2.7.exe](resources/binaries/pycrypto-2.6.win32-py2.7.exe)
4. `pip install fabric` again.  This should be successful.


Configuration Files
-------------------

The configuration files, located in [`resources/settings`](https://github.com/klahnakoski/ActiveData-ETL/tree/dev/resources/settings), often point to a `private.json` config file outside the repository tree.  This file holds the credentials and access info required, and looks something like this:

	{
	    "email":{
	        "host": "smtp.gmail.com",
	        "port": 465,
	        "username": "",
	        "password": "",
	        "use_ssl": 1
	    },
	    "aws_credentials":{
	        "aws_access_key_id":"",
	        "aws_secret_access_key" :"",
	        "region":"us-west-2"
	    },
	    "pulse_user":{
	        "user": "",
	        "password": ""
	    }
	}

The exact properties will depend on the the resources you are accessing. 