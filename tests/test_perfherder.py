# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from pyLibrary.aws import s3
from pyLibrary.dot import Null
from pyLibrary.jsons import ref
from pyLibrary.maths.randoms import Random
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from testlog_etl.sinks.s3_bucket import S3Bucket
from testlog_etl.transforms import pulse_block_to_perfherder_logs, perfherder_logs_to_perf_logs

false = False
true = True

class TestBuildbotLogs(FuzzyTestCase):

    def __init__(self, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)
        self.settings = ref.get("file://~/private.json");

    def test_capture(self):
        source_key = u'213657:13240348'
        source = s3.Bucket(bucket="active-data-pulse-beta", settings=self.settings.aws).get_key(source_key)
        dest_bucket = S3Bucket(bucket="active-data-perfherder-beta", settings=self.settings.aws)
        resources = Null
        pulse_block_to_perfherder_logs.process(source_key, source, dest_bucket, resources, please_stop=None)

    def test_perfherder_transform_a(self):
        source_key = u'241125:24077577'
        source = s3.Bucket(bucket="active-data-perfherder", settings=self.settings.aws).get_key(source_key)
        dest_bucket = S3Bucket(bucket="active-data-perf-dev", settings=self.settings.aws)
        resources = Null
        perfherder_logs_to_perf_logs.process(source_key, source, dest_bucket, resources, please_stop=None)

    def test_perfherder_transform_b(self):
        source_key = u'300042:29969274.1'
        source = s3.Bucket(bucket="active-data-perfherder", settings=self.settings.aws).get_key(source_key)
        dest_bucket = S3Bucket(bucket="active-data-perf-dev", settings=self.settings.aws)
        resources = Null
        perfherder_logs_to_perf_logs.process(source_key, source, dest_bucket, resources, please_stop=None)


    def test_perfherder_transform_c(self):
        source_key = u'307827:30747788.7'
        source = s3.Bucket(bucket="active-data-perfherder", settings=self.settings.aws).get_key(source_key)
        dest_bucket = S3Bucket(bucket="active-data-perf-dev", settings=self.settings.aws)
        resources = Null
        perfherder_logs_to_perf_logs.process(source_key, source, dest_bucket, resources, please_stop=None)



    def test_many_perfherder_transform(self):
        bucket = s3.Bucket(bucket="active-data-perfherder", settings=self.settings.aws)
        all_keys = (k.name.replace(".json.gz", "") for k in bucket.bucket.list(prefix="30"))
        for k in all_keys:
            if not Random.range(0, 10) == 0:
                continue

            source_key = k
            dest_bucket = S3Bucket(bucket="active-data-perf-dev", settings=self.settings.aws)
            resources = Null
            perfherder_logs_to_perf_logs.process(source_key, bucket.get_key(source_key), dest_bucket, resources, please_stop=None)

