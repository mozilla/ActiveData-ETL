# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (klahnakoski@mozilla.com)
#
from __future__ import division
from __future__ import unicode_literals

import unittest

from activedata_etl.transforms.jsdcov_to_es import process_jsdcov_artifact
from mo_dots import Null, Data
from mo_times import Date, Duration, WEEK
from test_gcov import Destination


class TestJsdov(unittest.TestCase):

    def test_one_url(self):
        key = Null
        url = "http://queue.taskcluster.net/v1/task/JFTo4WWfS3GGK8-A4y26Pw/artifacts/public/test_info//jsdcov_artifacts.zip"
        destination = Destination("results/jsdcov/lcov_parsing_result.json.gz")

        process_jsdcov_artifact(
            source_key=key,
            resources=Data(),
            destination=destination,
            artifact=Data(url=url),
            task_cluster_record=Data(repo={"push": {"date": Date.now()}}),
            artifact_etl=Null,
            please_stop=Null
        )
