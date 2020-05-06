# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from mo_testing.fuzzytestcase import FuzzyTestCase

false = False
true = True

class TestUnittestLogsToSink(FuzzyTestCase):

    def test_specif_url(self):
        url = "http://queue.taskcluster.net/v1/task/Izw-lZINTFqQsnnrv5N1UQ/artifacts/public/test_info//mochitest-devtools-chrome-chunked_raw.log"
