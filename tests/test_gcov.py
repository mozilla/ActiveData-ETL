# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: ActiveData Maintainers (active-data-maintainers@mozilla.com)
#
from __future__ import division
from __future__ import unicode_literals

import gzip
import unittest

from activedata_etl import key2etl
from mo_dots import Null, Data, wrap
from mo_files import File
from mo_logs import constants

from activedata_etl.imports import parse_lcov
from activedata_etl.transforms import gcov_to_es
from activedata_etl.transforms.gcov_to_es import process_directory, process_gcda_artifact


class TestGcov(unittest.TestCase):
    def test_parsing(self):
        parse_lcov.EMIT_RECORDS_WITH_ZERO_COVERAGE = True
        destination = Destination("results/ccov/gcov_parsing_result.json.gz")

        gcov_to_es.process_directory(
            "tc.0:0.0",
            source_dir="tests/resources/ccov/atk",
            # source_dir="/home/marco/Documenti/FD/mozilla-central/build-cov-gcc",
            destination=destination,
            task_cluster_record=Null,
            file_etl=key2etl("tc.123:12345.45.0"),
            please_stop=Null
        )

        self.assertEqual(destination.count, 174, "Expecting 174 records, got " + str(destination.count))


    def test_lcov_post_processing(self):
        destination = Destination("results/ccov/lcov_parsing_result.json.gz")
        constants.set({"activedata_etl": {"transforms": {"gcov_to_es": {"DEBUG_LCOV_FILE": File("results/ccov/lcov.txt")}}}})
        source_dir = File("results/ccov")
        process_directory(Null, source_dir, destination, Null, Null)

    def test_mochitest_chunk_2_processing(self):
        destination = Destination("results/mochitest_chunk_2/active_data.json.gz")
        constants.set({"activedata_etl": {"transforms": {"gcov_to_es": {"DEBUG_LCOV_FILE": File("tests/resources/mochitest_chunk_2/lcov.txt")}}}})
        source_dir = File("tests/resources/mochitest_chunk_2")
        process_directory(
            source_key=Null,
            source_dir=source_dir,
            destination=destination,
            task_cluster_record=Null,
            file_etl=Null,
            please_stop=Null
        )

    def test_one_gcda_url(self):
        key="tc.472127"
        url="https://queue.taskcluster.net/v1/task/PNzAZrN7SUeKMK0_-wZr0Q/runs/0/artifacts/public/test_info//code-coverage-gcda.zip"
        destination = Destination("results/ccov/lcov_parsing_result.json.gz")

        process_gcda_artifact(
            source_key=key,
            resources=Null,
            destination=destination,
            gcda_artifact=Data(url=url),
            task_cluster_record=wrap({"task":{"id": "PNzAZrN7SUeKMK0_-wZr0Q", "group": {"id": "bf1GXS-2Rj6JtIFfA632rQ"}}}),
            artifact_etl=Null,
            please_stop=Null
        )


