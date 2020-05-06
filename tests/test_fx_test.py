# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

import requests

from activedata_etl.transforms import fx_test_to_normalized
from mo_dots import Null, Data
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestFX(FuzzyTestCase):

    def test_sample(self):
        url = "https://s3.amazonaws.com/net-mozaws-stage-fx-test-activedata/jenkins-go-bouncer.prod-3019/py27.log"
        response = requests.get(url).content
        fx_test_to_normalized.process(Data(), response.split("\n"), Null, Data(url=url), Null)
    #
    # def test_scan(self):
    #     bucket = s3.PublicBucket("https://s3.amazonaws.com/net-mozaws-stage-fx-test-activedata")
    #     results = bucket.list()
    #     for r in results:
    #         Log.note("{{item}}", item=r)
