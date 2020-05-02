# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (klahnakoski@mozilla.com)
#
from __future__ import division
from __future__ import unicode_literals

import unittest


from activedata_etl.sinks import s3_bucket
from mo_logs import startup


class TestGcovS3(unittest.TestCase):
    def test_parsing(self):

        settings = startup.read_settings(filename="resources/settings/codecoverage/etl.json")
        destination = s3_bucket.S3Bucket(settings.workers[0].destination)

        # read from "results/ccov/gcov_parsing_result.txt"
        # to create a list of records

        destination.extend(records, overwrite=True)

        # gcov_to_es.process_directory(
        #     source_dir="tests/resources/ccov/atk",
        #     destination= s3_bucket.S3Bucket(settings.workers[0].destination),
        #     task_cluster_record=Null,
        #     file_etl=Null
        # )

        # check that S3 expand worked and the data was written to the S3 bucket
        # check etl keys from test file and make sure they are in S3 .find_keys()
