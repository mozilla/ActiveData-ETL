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

from mo_logs import Log
from pyLibrary.env import http
from mo_testing.fuzzytestcase import FuzzyTestCase
from activedata_etl.transforms.pulse_block_to_job_logs import process_text_log

false = False
true = True


class TestJobs(FuzzyTestCase):

    def test_new_format(self):
        url = "http://archive.mozilla.org/pub/firefox/tinderbox-builds/autoland-win32-pgo/1470036602/autoland_win7_ix_test_pgo-mochitest-clipboard-bm126-tests1-windows-build42.txt.gz"
        # url = "http://archive.mozilla.org/pub/firefox/tinderbox-builds/autoland-linux64-pgo/1468999988/autoland_ubuntu64_hw_test-g4-pgo-bm103-tests1-linux-build42.txt.gz"
        response = http.get(url)
        output = process_text_log(response.get_all_lines(encoding=None), url)
        Log.note("{{output|json}}", output=output)

    def test_old_format(self):
        url = "http://archive.mozilla.org/pub/firefox/tinderbox-builds/mozilla-central-linux-debug/1467971787/mozilla-central-linux-debug-bm74-build1-build108.txt.gz"
        response = http.get(url)
        output = process_text_log(response.all_lines, url)
        Log.note("{{output|json}}", output=output)
