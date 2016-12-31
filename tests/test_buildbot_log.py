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

import itertools

from activedata_etl.buildbot_json_logs import parse_day
from activedata_etl.imports import buildbot
from activedata_etl.imports.buildbot import BuildbotTranslator
from activedata_etl.transforms.pulse_block_to_job_logs import process_text_log
from pyLibrary import convert, jsons
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyDots import listwrap
from pyLibrary.env import http
from pyLibrary.env.files import File
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY

false = False
true = True


class TestBuildbotLogs(FuzzyTestCase):

    def __init__(self, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)

    # def test_one_s3_file(self):
    #     key = "550.108"
    #     settings = jsons.ref.get("resources/settings/dev_to_staging/etl.json")
    #     bucket_settings = [w for w in settings.workers if w.name=="bbb2jobs"][0].source
    #     source = s3.Bucket(bucket_settings).get_key(key)
    #
    #     buildbot_block_to_job_logs(key, source, Null, resources, Null)



    def test_past_problems(self):
        COMPARE_TO_EXPECTED = True

        translator = BuildbotTranslator()

        builds = convert.json2value(File("tests/resources/buildbot.json").read(), flexible=True)
        if COMPARE_TO_EXPECTED:
            expected = convert.json2value(File("tests/resources/buildbot_results.json").read(), flexible=True)
        else:
            expected = []

        results = []
        failures = []
        for i, (b, e) in enumerate(itertools.izip_longest(builds, expected)):
            if e != None:
                e.other = set(listwrap(e.other))
                e.properties.uploadFiles = set(listwrap(e.properties.uploadFiles))
            try:
                result = translator.parse(b)
                results.append(result)
                if COMPARE_TO_EXPECTED:
                    if e == None:
                        Log.error("missing expected output")
                    self.assertEqual(result, e)
            except Exception, e:
                e = Except.wrap(e)
                failures.append(e)
                Log.warning("problem", cause=e)

        if failures:
            Log.error("parsing problems", cause=failures)

        if not COMPARE_TO_EXPECTED:
            File("tests/resources/buildbot_results.json").write(convert.value2json(results, pretty=True))

    def test_by_key_day(self):
        day = 634
        date = Date("2015/01/01") + day * DAY
        filename = date.format("builds-%Y-%m-%d.js.gz")

        settings = jsons.ref.expand({
            "force": true,
            "source": {
                "url": "http://builddata.pub.build.mozilla.org/builddata/buildjson/"
            },
            "destination": {
                "bucket": "active-data-buildbot",
                "public": true,
                "$ref": "file://~/private.json#aws_credentials"
            },
            "notify": {
                "name": "active-data-etl",
                "$ref": "file://~/private.json#aws_credentials"
            },
            "constants": {
                "pyLibrary.env.http.default_headers": {
                    "Referer": "https://wiki.mozilla.org/Auto-tools/Projects/ActiveData",
                    "User-Agent": "ActiveData-ETL"
                }
            },
            "debug": {"cprofile": False}
        }, "file:///")

        Log.start(settings.debug)

        parse_day(settings, filename, force=True)

    def test_all_in_one_day(self):
        filename = "builds-2015-12-20.js.gz"

        settings = jsons.ref.expand({
            "force": false,
            "source": {
                "url": "http://builddata.pub.build.mozilla.org/builddata/buildjson/"
            },
            "destination": {
                "bucket": "active-data-buildbot",
                "public": true,
                "$ref": "file://~/private.json#aws_credentials"
            },
            "notify": {
                "name": "active-data-etl",
                "$ref": "file://~/private.json#aws_credentials"
            },
            "constants": {
                "pyLibrary.env.http.default_headers": {
                    "Referer": "https://wiki.mozilla.org/Auto-tools/Projects/ActiveData",
                    "User-Agent": "ActiveData-ETL"
                }
            }
        }, "file:///")
        parse_day(settings, filename, force=True)

    def test_decode_quoted_dict(self):
        test = "[{u'url': u'http://ftp.mozilla.org/pub/mozilla.org/firefox/nightly/2015/07/2015-07-09-00-40-07-mozilla-aurora/firefox-41.0a2.en-US.linux-x86_64.partial.20150708004005-20150709004007.mar', u'hash': u'0e4c731b2c9089a8c085d6abbeffa09aeaac4a142c6caed094c64f62c639143f27dc8d5ee2fddb988e5ea208a25a178f6d7fa8cf3e293375b493eab16ac1f71f', u'from_buildid': u'20150708004005', u'size': 5427986}]"
        expecting = [{
                         u'url': u'http://ftp.mozilla.org/pub/mozilla.org/firefox/nightly/2015/07/2015-07-09-00-40-07-mozilla-aurora/firefox-41.0a2.en-US.linux-x86_64.partial.20150708004005-20150709004007.mar',
                         u'hash': u'0e4c731b2c9089a8c085d6abbeffa09aeaac4a142c6caed094c64f62c639143f27dc8d5ee2fddb988e5ea208a25a178f6d7fa8cf3e293375b493eab16ac1f71f',
                         u'from_buildid': u'20150708004005',
                         u'size': 5427986
                     }]

        result = buildbot.unquote(test)
        self.assertEqual(result, expecting)

    def test_specific_url(self):
        # url = "http://archive.mozilla.org/pub/mobile/tinderbox-builds/fx-team-android-api-15-debug/1461301083/fx-team_ubuntu64_vm_armv7_large-debug_test-plain-reftest-12-bm114-tests1-linux64-build15.txt.gz"
        url = "http://archive.mozilla.org/pub/firefox/tinderbox-builds/mozilla-inbound-win64-pgo/1462512703/mozilla-inbound_win8_64_test_pgo-web-platform-tests-5-bm126-tests1-windows-build22.txt.gz"
        response = http.get(url)
        # response = http.get("http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win32/1444321537/mozilla-inbound_xp-ix_test-g2-e10s-bm119-tests1-windows-build710.txt.gz")
        # for i, l in enumerate(response._all_lines(encoding=None)):
        #     try:
        #         l.decode('latin1').encode('utf8')
        #     except Exception:
        #         Log.alert("bad line {{num}}", num=i)
        #
        #     Log.note("{{line}}", line=l)

        data = process_text_log(response.get_all_lines(encoding=None), url)
        Log.note("{{data}}", data=data)
