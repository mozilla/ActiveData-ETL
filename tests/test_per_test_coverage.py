# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Marco Castelluccio (mcastelluccio@mozilla.com)
#
from __future__ import division
from __future__ import unicode_literals

import unittest

from activedata_etl.transforms.per_test_to_es import process_per_test_artifact
from mo_dots import Null, Data
from mo_times import Date
from test_gcov import Destination


class TestPerTestCoverage(unittest.TestCase):

    def test_one_url(self):
        key = Null
        url = "https://taskcluster-artifacts.net/ACw-xlQvSRa2A6Hvv7bNtA/0/public/test_info//per-test-coverage-reports.zip"
        destination = Destination("results/per_test_coverage/parsing_result.json.gz")

        process_per_test_artifact(
            source_key=key,
            resources=Data(),
            destination=destination,
            artifact=Data(url=url),
            task_cluster_record=Data(repo={"push": {"date": Date.now()}}),
            artifact_etl=Null,
            please_stop=Null
        )

        self.assertEqual(destination.count, 16494, "Expecting 16494 records, got " + str(destination.count))
