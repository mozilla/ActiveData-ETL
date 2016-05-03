
Code Coverage Settings
======================

Settings for the code coverage project.

AWS Permissions
---------------

AWS Policies are powerful, but the details evade documentation.  Here is the policy for the `codecoverage` user.  The use of stars (`*`) is important:

1. Resources must end with stars, they do not match exactly
2. Stars are only allowed as a suffix, prefix does not match
3. Resources have the form `arn:aws:A:B:C:D`.  You may need a star (`*`) in each of the `B` and `C` positions, depending on the resource family.  For example, `arn:aws:sqs` requires stars, `arn:aws:s3` does not.   


	
	{
	    "Version": "2012-10-17",
	    "Statement": [
	        {
	            "Effect": "Allow",
	            "Action": "s3:*",
	            "Resource": [
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
	            "Effect": "Allow",
	            "Action": [
	                "s3:Get*"
	            ],
	            "Resource": [
	                "arn:aws:s3:::*"
	            ]
	        },
	        {
	            "Effect": "Allow",
	            "Action": [
	                "s3:List*"
	            ],
	            "Resource": [
	                "arn:aws:s3:::*"
	            ]
	        },
	        {
	            "Action": [
	                "sqs:*"
	            ],
	            "Effect": "Allow",
	            "Resource": [
	                "arn:aws:sqs:*:*:active-data-etl-dev*",
	                "arn:aws:sqs:*:*:active-data-index-jobs-dev*",
	                "arn:aws:sqs:*:*:active-data-index-perf-dev*",
	                "arn:aws:sqs:*:*:active-data-index-unit-dev*"
	            ]
	        },
	        {
	            "Action": [
	                "sqs:List*"
	            ],
	            "Effect": "Allow",
	            "Resource": "arn:aws:sqs:*:*:*"
	        },
	        {
	            "Action": [
	                "sqs:Get*"
	            ],
	            "Effect": "Allow",
	            "Resource": "arn:aws:sqs:*:*:*"
	        }
	    ]
	}