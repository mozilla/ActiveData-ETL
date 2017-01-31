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
from mo_logs import Log
from mo_dots import Null, listwrap, Data, wrap
from pyLibrary.env import http
from mo_json import ref
from mo_math.randoms import Random
from mo_testing.fuzzytestcase import FuzzyTestCase
from activedata_etl.sinks.s3_bucket import S3Bucket
from activedata_etl.transforms import pulse_block_to_perfherder_logs, perfherder_logs_to_perf_logs, EtlHeadGenerator
from activedata_etl.transforms.perfherder_logs_to_perf_logs import stats
from activedata_etl.transforms.pulse_block_to_perfherder_logs import extract_perfherder

false = False
true = True

class TestBuildbotLogs(FuzzyTestCase):

    def __init__(self, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)
        self.settings = ref.get("file://~/private.json");

    def test_url(self):
        url = "http://archive.mozilla.org/pub/firefox/tinderbox-builds/mozilla-inbound-win64/1469025080/mozilla-inbound_win8_64_test-svgr-e10s-bm127-tests1-windows-build1138.txt.gz"

        def dummy(a, b):
            return Null, Null
        seen, all_perf = extract_perfherder(http.get(url).all_lines, Null, Data(next=dummy), Null, Null)
        Log.note("{{output}}", output=all_perf)


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

    def test_perfherder_transform_d(self):
        url = "https://archive.mozilla.org/pub/thunderbird/tinderbox-builds/comm-central-win64/1474894430/comm-central-win64-bm77-build1-build0.txt.gz"
        response = http.get(url)
        pulse_block_to_perfherder_logs.extract_perfherder(response.get_all_lines(flexible=True), Null, Null, None, Null)

    def test_perfherder_transform_e(self):
        url = "https://archive.mozilla.org/pub/firefox/tinderbox-builds/mozilla-inbound-macosx64/1475228359/mozilla-inbound_yosemite_r7_test-tp5o-bm106-tests1-macosx-build3011.txt.gz"
        etl_head_gen = EtlHeadGenerator(Null)
        response = http.get(url)
        pulse_block_to_perfherder_logs.extract_perfherder(response.get_all_lines(flexible=True), Null, etl_head_gen, None, Null)

    def test_perfherder_job_resource_usage(self):
        data = '{"framework": {"name": "job_resource_usage"}, "suites": [{"subtests": [{"name": "cpu_percent", "value": 15.91289772727272}, {"name": "io_write_bytes", "value": 340640256}, {"name": "io.read_bytes", "value": 40922112}, {"name": "io_write_time", "value": 6706180}, {"name": "io_read_time", "value": 212030}], "extraOptions": ["e10s"], "name": "mochitest.mochitest-devtools-chrome.1.overall"}, {"subtests": [{"name": "time", "value": 2.5980000495910645}, {"name": "cpu_percent", "value": 10.75}], "name": "mochitest.mochitest-devtools-chrome.1.install"}, {"subtests": [{"name": "time", "value": 0.0}], "name": "mochitest.mochitest-devtools-chrome.1.stage-files"}, {"subtests": [{"name": "time", "value": 440.6840000152588}, {"name": "cpu_percent", "value": 15.960411899313495}], "name": "mochitest.mochitest-devtools-chrome.1.run-tests"}]}'
        # data = wrap({"framework": {"name": "job_resource_usage"}, "suites": [{"subtests": [{"name": "cpu_percent", "value": 15.91289772727272}, {"name": "io_write_bytes", "value": 340640256}, {"name": "io.read_bytes", "value": 40922112}, {"name": "io_write_time", "value": 6706180}, {"name": "io_read_time", "value": 212030}], "extraOptions": ["e10s"], "name": "mochitest.mochitest-devtools-chrome.1.overall"}, {"subtests": [{"name": "time", "value": 2.5980000495910645}, {"name": "cpu_percent", "value": 10.75}], "name": "mochitest.mochitest-devtools-chrome.1.install"}, {"subtests": [{"name": "time", "value": 0.0}], "name": "mochitest.mochitest-devtools-chrome.1.stage-files"}, {"subtests": [{"name": "time", "value": 440.6840000152588}, {"name": "cpu_percent", "value": 15.960411899313495}], "name": "mochitest.mochitest-devtools-chrome.1.run-tests"}]})
        perfherder_logs_to_perf_logs.process("dummy", wrap_as_bucket([data]), Null, Null, Null)

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

    def test_stats(self):
        results = stats(Null, [float("nan"), 1, 2, 3, 4, 5])
        self.assertEqual(results.stats.count, 5)
        self.assertEqual(len(listwrap(results.stats.rejects)), 1)


    def test_warning(self):
        values=[float("nan"), 42]
        Log.warning("problem {{values|json}}", values=values)


def wrap_as_bucket(data):
    def read_lines():
        return data
    return Data(read_lines=read_lines)
