# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from activedata_etl.imports.task import decode_metatdata_name
from mo_dots import Null, unwrap
from mo_files import File
from mo_json import value2json
from mo_logs import Log
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times import Timer

OVERWRITE_RESOURCE = True

# FIND MORE NAMES:
# {
#     "from":"debug-etl",
#     "groupby":"params.name",
#     "where":{"and":[
#         {"eq":{"template":"{{name|quote}} can not be processed with {{category}} for key {{key}}"}},
#         {"gte":{"timestamp":{"date":"today-2week"}}}
#     ]},
#     "limit":1000
# }

class TestMetadataName(FuzzyTestCase):
    def test_basic(self):
        Log.alert("If you see any results, then you have OVERWRITE_RESOURCE = True and tests are FAILING")
        with Timer("test time"):
            resource = File("tests/resources/metadata_names.json")
            tests = unwrap(resource.read_json(leaves=False, flexible=False))
            for name, expected in list(tests.items()):
                result = decode_metatdata_name(Null, name)

                if OVERWRITE_RESOURCE:
                    tests[name] = result
                else:
                    self.assertEqual(result, expected)
                    self.assertEqual(expected, result)

            if OVERWRITE_RESOURCE:
                resource.write_bytes(value2json(tests, pretty=True).encode("utf8"))

    def test_one(self):
        test = decode_metatdata_name(
            Null, "test-windows10-64-shippable/opt-browsertime-tp6-1-chrome-cold-e10s"
        )
        expected = {
            "action": {"type": "raptor"},
            "build": {"type": ["opt"], "platform": "linux64"},
            "run": {
                "type": ["e10s"],
                "suite": {"name": "wasm-misc-ion"},
                "browser": "firefox",
            },
        }

        self.assertEqual(test, expected)
        self.assertEqual(expected, test)

    def test_one2(self):
        test = decode_metatdata_name(Null, "test-linux64/debug-reftest-stylo-8")
        expected = {
            "action": {"type": "test"},
            "build": {"type": ["stylo", "debug"], "platform": "linux64"},
            "run": {"suite": {"name": "reftest"}, "chunk": 8, "type": ["chunked"]},
        }

        self.assertEqual(test, expected)
        self.assertEqual(expected, test)
