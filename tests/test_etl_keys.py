# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from testlog_etl import key2etl


class TestETLKeys(FuzzyTestCase):
    def test_key_2_etl(self):
        self.assertEqual(key2etl("1:2"), {"id": 1, "source": {"id": 2}, "type": "aggregation"})
        self.assertEqual(key2etl("1:2.3"), {"id": 3, "source": {"id": 1, "source": {"id": 2}, "type": "aggregation"}, "type": "join"})

        self.assertEqual(key2etl("1:2.3.4"), {
            "id": 4,
            "source": {
                "id": 3,
                "source": {
                    "id": 1,
                    "source": {"id": 2},
                    "type": "aggregation"
                },
                "type": "join"
            }, "type": "join"
        })
