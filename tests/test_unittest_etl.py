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

from activedata_etl.transforms.unittest_logs_to_sink import process_unittest
from mo_dots import Null, Data
from mo_testing.fuzzytestcase import FuzzyTestCase
from pyLibrary.env import http
from tests import Destination

false = False
true = True


class TestUnittestETL(FuzzyTestCase):

    def test_one_file(self):
        url = "https://queue.taskcluster.net/v1/task/IRc89KVySoiyyn61ZVNEGA/artifacts/public/test_info/reftest_raw.log"

        response = http.get(url)

        process_unittest(
            source_key=Null,
            etl_header=Null,
            buildbot_summary=Data(run={"suite": {"name": "reftest"}}),
            unittest_log=response.get_all_lines(),
            destination=Destination("result/output.txt"),
            please_stop=None
        )

