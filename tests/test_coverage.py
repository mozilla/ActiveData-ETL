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

from mo_files import File
from mo_json import stream, json2value
from mo_logs import Log
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times import Date


class TestCoverage(FuzzyTestCase):


    def test_missing_method_level(self):
        url = "https://public-artifacts.taskcluster.net/JRGVUfo_RGiU_D0RhKomMQ/0/public/test_info//jscov_1469916625145.json"
        response = requests.get(url, stream=True)
        _stream = response.raw.stream()
        records = []
        for source_file_index, obj in enumerate(stream.parse(_stream, [], ["."])):
            if source_file_index==0:
                continue  # VERSION LINE
            # records = list(process_jsdcov_artifact(Data(), obj, Null, Null, Null, Null)
        Log.note("{{records|json}}", records=records)

    def test_read_coverage(self):
        Date.now()
        count = 0
        total = 0
        filename = "C:/Users/kyle/code/Activedata-ETL/results/tc.696110_69610163.19.0.json"
        for line in File(filename).read_lines():
            d = json2value(line)
            total += 1
            if d.source.file.total_covered > 0:
                Log.note("\n{{_id}}, {{run.type}}, {{source.file.total_covered}}, {{source.file.name}}, {{test}}", default_params=d)
                count += 1
        Log.note("{{num}} records with lines covered out of {{total}}", num=count, total=total)
