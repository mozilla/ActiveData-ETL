# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division

from pyLibrary import convert
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.env.files import File
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from testlog_etl.imports import buildbot
from testlog_etl.imports.buildbot import BuildbotTranslator
from testlog_etl.transforms.pulse_block_to_job_logs import process_buildbot_log

false = False
true = True

class TestBuildbotLogs(FuzzyTestCase):

    def __init__(self, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)

    def test_past_problems(self):
        t = BuildbotTranslator()

        builds = convert.json2value(File("tests/resources/buildbot.json").read())
        failures=[]
        for b in builds:
            try:
                t.parse(b)
            except Exception, e:
                e = Except.wrap(e)
                failures.append(e)
                Log.warning("problem", cause=e)

        if failures:
            Log.error("parsing problems", cause=failures)

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
        url = "http://archive.mozilla.org/pub/b2g/tinderbox-builds/mozilla-central-emulator-debug/1453460790/mozilla-central_ubuntu64_vm-b2g-emulator-debug_test-mochitest-debug-10-bm124-tests1-linux64-build10.txt.gz"
        response = http.get(url)
        # response = http.get("http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win32/1444321537/mozilla-inbound_xp-ix_test-g2-e10s-bm119-tests1-windows-build710.txt.gz")
        # for i, l in enumerate(response._all_lines(encoding="latin1")):
        #     try:
        #         l.decode('latin1').encode('utf8')
        #     except Exception:
        #         Log.alert("bad line {{num}}", num=i)
        #
        #     Log.note("{{line}}", line=l)

        data = process_buildbot_log(response.all_lines, "<unknown>")
        Log.note("{{data}}", data=data)
