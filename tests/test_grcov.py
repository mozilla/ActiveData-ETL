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

import unittest

from activedata_etl.imports.tuid_client import TuidClient
from activedata_etl.transforms.grcov_to_es import process_grcov_artifact
from mo_dots import Null, Data
from test_gcov import Destination


class TestGcov(unittest.TestCase):

    def test_one(self):
        url = "http://queue.taskcluster.net/v1/task/a-LgV-cVTKiDxjl5I_4tWg/artifacts/public/test_info/code-coverage-grcov.zip"

        resources = Data(
            file_mapper=Data(find=fake_file_mapper),
            tuid_mapper=TuidClient("http://54.149.21.8/tuid")
        )

        destination = Destination("results/grcov/parsing_result.json.gz")

        process_grcov_artifact(
            source_key=Null,
            resources=resources,
            destination=destination,
            grcov_artifact=Data(url=url),
            task_cluster_record=Null,
            artifact_etl=Null,
            please_stop=Null
        )


def fake_file_mapper(source_key, filename, grcov_artifact, task_cluster_record):
    return {"name": filename}
