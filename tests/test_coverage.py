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

import requests
from mo_dots import Null, Data
from mo_json import stream
from mo_logs import Log

from activedata_etl.transforms.jscov_to_es import process_source_file
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestCoverage(FuzzyTestCase):


    def test_missing_method_level(self):
        url = "https://public-artifacts.taskcluster.net/JRGVUfo_RGiU_D0RhKomMQ/0/public/test_info//jscov_1469916625145.json"
        response = requests.get(url, stream=True)
        _stream = response.raw.stream()
        records = []
        for source_file_index, obj in enumerate(stream.parse(_stream, [], ["."])):
            if source_file_index==0:
                continue  # VERSION LINE
            process_source_file(Data(), obj, Null, Null, Null, Null, records)
        Log.note("{{records|json}}", records=records)

