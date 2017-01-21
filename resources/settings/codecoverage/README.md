
Code Coverage Settings
======================

Settings for the code coverage project.

Requirements
------------

* Local installation of Elasticsearch 1.7.x (NOT A RECENT VERSION)

* Forked-and-cloned copy of `git clone https://github.com/klahnakoski/ActiveData-ETL.git`


Install
-------

Fork the ActiveData-ETL repo, then clone it locally:

	git clone https://github.com/klahnakoski/ActiveData-ETL.git

be sure to use the `codecoverage` branch

	git checkout codecoverage

during development you will occasionally pull changes other made to the branch

	git pull origin codecoverage

and you will push your own changes

	git push origin codecoverage


There are several dependencies you will need before you can "pip install" all the requirements.  Here is the script I use on the production machines.

	sudo yum group install "Development Tools"
	sudo yum install -y libffi-devel
	sudo yum install -y openssl-devel
	
	sudo /usr/local/bin/pip install ecdsa
	sudo /usr/local/bin/pip install fabric
	sudo /usr/local/bin/pip install -r requirements.txt


Configuration
-------

You must make your own `codecoverage.json` file. The config files in the repo assume this file is in your home directory `~/codecoverage.json`.  Wherever you put it, it should be in a safe place; not in your cloned repo directory where it may get picked up by Git and pushed to the public repo. It will contain the important keys to access various services.

	{
	    "email":{
	        "host": "smtp.gmail.com",
	        "port": 465,
	        "username": "klahnakoski@mozilla.com",
	        "password": "password",
	        "use_ssl": 1
	    },
	    "ssl_context": "adhoc",
	    "aws_credentials":{
	        "aws_access_key_id":"blah",
	        "aws_secret_access_key" :"blah",
	        "region":"us-west-2"
	    },
	    "pulse_user":{
	        "user": "ekyle-aws-1",
	        "password": "blah"
	    }
	}






AWS Permissions
---------------

AWS Policies are powerful, but the details evade documentation.  Here is the policy for the `codecoverage` user.  The use of stars (`*`) is important:

1. Resources must end with stars, they do not match exactly
2. Stars are only allowed as a suffix, prefix does not match
3. Resources have the form `arn:aws:A:B:C:D`.  You may need a star (`*`) in each of the `B` and `C` positions, depending on the resource family.  For example, `arn:aws:sqs` requires stars, `arn:aws:s3` does not.


<pre>
{
    "Version":"2012-10-17",
    "Statement":[
        {
            "Effect":"Allow",
            "Action":"s3:*",
            "Resource":[
                "arn:aws:s3:::active-data-buildbot-dev*",
                "arn:aws:s3:::active-data-jobs-dev*",
                "arn:aws:s3:::active-data-perf-dev*",
                "arn:aws:s3:::active-data-perfherder-dev*",
                "arn:aws:s3:::active-data-pulse-dev*",
                "arn:aws:s3:::active-data-talos-dev*",
                "arn:aws:s3:::active-data-test-result-dev*"
            ]
        },
        {
            "Effect":"Allow",
            "Action":["s3:Get*"],
            "Resource":["arn:aws:s3:::*"]
        },
        {
            "Effect":"Allow",
            "Action":["s3:List*"],
            "Resource":["arn:aws:s3:::*"]
        },
        {
            "Action":["sqs:*"],
            "Effect":"Allow",
            "Resource":[
                "arn:aws:sqs:*:*:active-data-etl-dev*",
                "arn:aws:sqs:*:*:active-data-index-jobs-dev*",
                "arn:aws:sqs:*:*:active-data-index-perf-dev*",
                "arn:aws:sqs:*:*:active-data-index-unit-dev*"
            ]
        },
        {
            "Action":["sqs:List*"],
            "Effect":"Allow",
            "Resource":"arn:aws:sqs:*:*:*"
        },
        {
            "Action":["sqs:Get*"],
            "Effect":"Allow",
            "Resource":"arn:aws:sqs:*:*:*"
        }
    ]
}
</pre>
