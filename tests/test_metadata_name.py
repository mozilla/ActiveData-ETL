
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
from mo_dots import Null
from mo_files import File
from mo_json import value2json
from mo_testing.fuzzytestcase import FuzzyTestCase

OVERWRITE_RESOURCE = True


class TestMetadataName(FuzzyTestCase):

    def test_basic(self):
        resource = File("tests/resources/metadata_names.json")
        tests = resource.read_json()
        for name, expected in list(tests.items()):
            result = decode_metatdata_name(Null, name)

            if OVERWRITE_RESOURCE:
                tests[name]=result
            else:
                self.assertEqual(result, expected)
                self.assertEqual(expected, result)

        if OVERWRITE_RESOURCE:
            resource.write_bytes(value2json(tests, pretty=True).encode('utf8'))

