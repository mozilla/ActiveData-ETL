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

from activedata_etl.imports.task import minimize_task
from activedata_etl.transforms.grcov_to_es import process_grcov_artifact
from mo_dots import Null, Data
from pyLibrary.env import http
from test_gcov import Destination
from tuid.client import TuidClient

http.default_headers['Referer'] = "ActiveData testing"


class TestGrcov(unittest.TestCase):

    def test_one(self):
        url = "http://queue.taskcluster.net/v1/task/a-LgV-cVTKiDxjl5I_4tWg/artifacts/public/test_info/code-coverage-grcov.zip"

        source_key = http.get_json(
            "http://activedata.allizom.org/query",
            json={
                "from": "task.task.artifacts",
                "select": "_id",
                "where": {"eq": {"url": url}},
                "format": "list"
            }

        ).data[0]

        task_cluster_record = http.get_json(
            "http://activedata.allizom.org/query",
            json={
                "from": "task",
                "where": {"eq": {"_id": source_key}},
                "format": "list"
            }
        ).data[0]
        minimize_task(task_cluster_record)

        resources = Data(
            file_mapper=Data(find=fake_file_mapper),
            # file_mapper=FileMapper(task_cluster_record),
            tuid_mapper=TuidClient(endpoint="http://localhost:5000/tuid", timeout=30)
        )

        destination = Destination("results/grcov/parsing_result.json.gz")

        process_grcov_artifact(
            source_key=source_key,
            resources=resources,
            destination=destination,
            grcov_artifact=Data(url=url),
            task_cluster_record=task_cluster_record,
            artifact_etl=Null,
            please_stop=Null
        )


def fake_file_mapper(source_key, filename, grcov_artifact, task_cluster_record):
    return {"name": filename}
