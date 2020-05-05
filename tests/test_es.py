# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

import json
import unittest

import requests

from pyLibrary import jsons
from mo_testing import elasticsearch

ID = "tc.480019:48001141.30"
TASK_ID = "ER70OtGBQla6YOW5qeivnw"
DATA = {"task": {"id": TASK_ID}, "_id": ID}


class TestES(unittest.TestCase):

    def test_tc_record(self):

        es_settings = jsons.ref.expand({
            "host": "http://localhost",
            "port": 9200,
            "index": "test_es",
            "type": "task",
            "timeout": 300,
            "consistency": "one",
            "schema": {
                "$ref": "//../resources/schema/task_cluster.json"
            },
            "debug": True,
            "limit_replicas": True
        }, "file://./test_es.py")

        es = elasticsearch.Cluster(es_settings).create_index(es_settings)
        es.add({"id": ID, "value": DATA})
        es.refresh()

        # CONFIRM IT EXISTS
        query = {"query": {"filtered": {"filter": {"term": {"_id": ID}}}}}
        while True:
            try:
                result = es.search(query)
                self.assertEqual(result["hits"]["hits"][0]["_id"], ID, "Should not happen; expecting data to exists before test is run")
                print("Data exists, ready to test")
                break
            except Exception as e:
                print("waiting for data")

        query = {
            "query": {"filtered": {"filter": {"term": {"task.id": TASK_ID}}}},
            "from": 0,
            "size": 10

        }
        result = es.search(query)
        self.assertGreaterEqual(len(result["hits"]["hits"]), 1, "Expecting a record to be returned")
        self.assertEqual(result["hits"]["hits"][0]["_id"], ID, "Expecting particular record")

    def test_tc_record_basic(self):
        requests.delete("http://localhost:9200/test_es/")

        requests.post(
            url="http://localhost:9200/test_es",
            data=json.dumps({
                "mappings": {"task": {"properties": {"task": {
                    "type": "object",
                    "dynamic": True,
                    "properties": {
                        "id": {
                            "type": "string",
                            "index": "not_analyzed",
                            "doc_values": True
                        }
                    }
                }}}}
            })
        )

        # ADD RECORD TO ES
        requests.post(
            url="http://localhost:9200/test_es/task/_bulk",
            data=(
                json.dumps({"index": {"_id": ID}}) + "\n" +
                json.dumps(DATA) + "\n"
            )
        )
        requests.post("http://localhost:9200/test_es/_refresh")

        # CONFIRM IT EXISTS
        query = {"query": {"filtered": {"filter": {"term": {"_id": ID}}}}}
        while True:
            try:
                result = json.loads(
                    requests.post(
                        url="http://localhost:9200/test_es/_search",
                        data=json.dumps(query)
                    ).content.decode('utf8'))
                self.assertEqual(result["hits"]["hits"][0]["_id"], ID, "Should not happen; expecting data to exists before test is run")
                print("Data exists, ready to test")
                break
            except Exception:
                print("waiting for data")

        query = {
            "query": {"filtered": {"filter": {"term": {"task.id": TASK_ID}}}},
            "from": 0,
            "size": 10

        }
        result = json.loads(
            requests.post(
                url="http://localhost:9200/test_es/task/_search",
                data=json.dumps(query)
            ).content.decode('utf8'))
        self.assertGreaterEqual(len(result["hits"]["hits"]), 1, "Expecting a record to be returned")
        self.assertEqual(result["hits"]["hits"][0]["_id"], ID, "Expecting particular record")
