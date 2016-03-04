# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from collections import Mapping

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import set_default
from pyLibrary.env import http
from pyLibrary.strings import expand_template
from pyLibrary.times.dates import Date
from testlog_etl import etl2key

DEBUG = True

STATUS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
RETRY ={"times": 3, "sleep": 5}


def process(source_key, source, destination, resources, please_stop=None):
    etl_id = 0
    output = []

    lines = source.read_lines()
    for i, line in enumerate(lines):
        try:
            tc_message = convert.json2value(line)
            etl = tc_message.etl
            tc_message.etl = None
            taskid = tc_message.status.taskId
            Log.note("{{id}} found (#{{num}})", id=taskid, num=etl_id)

            # get the artifact list for the taskId
            artifacts = http.get_json(expand_template(ARTIFACTS_URL, {"task_id": taskid}), retry=RETRY)
            task = http.get_json(expand_template(STATUS_URL, {"task_id": taskid}), retry=RETRY)
            task.artifacts = artifacts.artifacts
            task.pulse = tc_message
            task.etl = {
                "id": etl_id,
                "source": etl,
                "timestamp": Date.now()
            }
            etl_id += 1

            for a in task.artifacts:
                a.url = expand_template(ARTIFACT_URL, {"task_id": taskid, "path": a.name})
                if a.name.endswith("/live_backing.log"):
                    a.main_log = True
                if a.name.endswith("_raw.log") and not a.name.endswith("/log_raw.log"):
                    a.structured = True

            _normalize(task)

            output.append(task)
        except Exception, e:
            Log.warning("problem", cause=e)

    destination.extend({"id": etl2key(t.etl), "value": t} for t in output)


def _normalize(task):
    task.payload.artifacts = _object_to_array(task.payload.artifacts, "name")
    task.payload.cache = _object_to_array(task.payload.cache, "name", "path")

    _scrub(task)


def _object_to_array(value, key_name, value_name=None):
    if value_name==None:
        return [set_default(v, {key_name: k}) for k, v in value.items()]
    else:
        return [{key_name: k, value_name: v} for k, v in value.items()]


def _scrub(doc):
    if isinstance(doc, Mapping):
        for k, v in doc.items():
            doc[k] = _scrub(v)
    elif isinstance(doc, list):
        for i, v in enumerate(doc):
            doc[i] = _scrub(v)
    elif isinstance(doc, basestring):
        try:
            return Date(doc).unix
        except Exception, e:
            pass

    return doc
