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
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from testlog_etl.sinks.s3_bucket import S3Bucket
from testlog_etl.transforms import pulse_block_to_perfherder_logs

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

