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

from pyLibrary import convert, jsons
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.env.files import File
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from testlog_etl.buildbot_json_jogs import parse_day
from testlog_etl.imports import buildbot
from testlog_etl.imports.buildbot import BuildbotTranslator
from testlog_etl.transforms.pulse_block_to_job_logs import process_buildbot_log

false = False
true = True


class TestBuildbotLogs(FuzzyTestCase):

    def __init__(self, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)

    def test_past_problems(self):
        COMPARE_TO_EXPECTED = True

        t = BuildbotTranslator()

        builds = convert.json2value(File("tests/resources/buildbot.json").read())
        if COMPARE_TO_EXPECTED:
            expected = convert.json2value(File("tests/resources/buildbot_results.json").read())
        else:
            expected = []

        results = []
        failures = []
        for i, (b, e) in enumerate(itertools.izip_longest(builds, expected)):
            try:
                result = t.parse(b)
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
                    "User-Agent": "testlog-etl"
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
        url = "http://archive.mozilla.org/pub/firefox/tinderbox-builds/fx-team-linux64/1453474887/fx-team_ubuntu64_vm_test-web-platform-tests-3-bm124-tests1-linux64-build7.txt.gz"
        response = http.get(url)
        # response = http.get("http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win32/1444321537/mozilla-inbound_xp-ix_test-g2-e10s-bm119-tests1-windows-build710.txt.gz")
        # for i, l in enumerate(response._all_lines(encoding="latin1")):
        #     try:
        #         l.decode('latin1').encode('utf8')
        #     except Exception:
        #         Log.alert("bad line {{num}}", num=i)
        #
        #     Log.note("{{line}}", line=l)
        try:
            data = process_buildbot_log(response.all_lines, "<unknown>")
        finally:
            response.close()
        Log.note("{{data}}", data=data)
