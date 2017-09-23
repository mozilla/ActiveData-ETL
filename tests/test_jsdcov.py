# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (klahnakoski@mozilla.com)
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
from activedata_etl.transforms import gcov_to_es, cov_to_es
from activedata_etl.transforms import jscov_to_es
from activedata_etl.transforms.gcov_to_es import process_directory, process_gcda_artifact
from pyLibrary.aws.s3 import PublicBucket
from test_gcov import Destination

class TestJSDCov(unittest.TestCase):

    def test_one_url(self):
        key="tc.472127"
        url="https://public-artifacts.taskcluster.net/ZFX36wSpS1iuQLzhFVqNhQ/0/public/test_info//jscov_1506005828494.json"
        destination = Destination("results/jscov/lcov_parsing_result.json.gz")

        jscov_to_es.process_jscov_artifact(
            source_key=key,
            resources=Null,
            destination=destination,
            artifact=Data(url=url),
            task_cluster_record=Null,
            artifact_etl=Null,
            please_stop=Null
        )

    def test_etl_block(self):
        source = Data(read_lines=lambda: PublicBucket(
            "https://s3-us-west-2.amazonaws.com/active-data-task-cluster-normalized").read_lines(
            "tc.1051816:105180763.json.gz"))
        destination = Destination("results/jscov/lcov_output.gz")

        cov_to_es.process(
            "tc.1051816",
            source=source,
            destination=destination,
            resources=Null,
        )