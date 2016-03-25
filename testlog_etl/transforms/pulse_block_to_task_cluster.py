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
from pyLibrary.dot import set_default, coalesce, Dict, unwraplist
from pyLibrary.env import http
from pyLibrary.strings import expand_template
from pyLibrary.testing.fuzzytestcase import assertAlmostEqual
from pyLibrary.times.dates import Date
from testlog_etl import etl2key

DEBUG = True
MAX_THREADS = 5

STATUS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
RETRY = {"times": 3, "sleep": 5}


seen = {}

def process(source_key, source, destination, resources, please_stop=None):
    output = []
    etl_source = None

    lines = source.read_lines()
    for i, line in enumerate(lines):
        try:
            tc_message = convert.json2value(line)
            taskid = tc_message.status.taskId
            if tc_message.artifact:
                continue
            Log.note("{{id}} w {{artifact}} found (#{{num}})", id=taskid, num=i, artifact=tc_message.artifact.name)

            task = http.get_json(expand_template(STATUS_URL, {"task_id": taskid}), retry=RETRY)
            normalized = _normalize(tc_message, task)

            # get the artifact list for the taskId
            artifacts = http.get_json(expand_template(ARTIFACTS_URL, {"task_id": taskid}), retry=RETRY).artifacts
            for a in artifacts:
                a.url = expand_template(ARTIFACT_URL, {"task_id": taskid, "path": a.name})
            normalized.task.artifacts=artifacts

            # FIX THE ETL
            etl = tc_message.etl
            etl_source = coalesce(etl_source, etl.source)
            etl.source = etl_source
            normalized.etl = {
                "id": i,
                "source": etl,
                "type": "join",
                "timestamp": Date.now()
            }

            tc_message.artifact="." if tc_message.artifact else None
            if normalized.task.id in seen:
                try:
                    assertAlmostEqual([tc_message, task, artifacts], seen[normalized.task.id], places=11)
                except Exception, e:
                    Log.error("Not expected", cause=e)
            else:
                tc_message._meta=None
                tc_message.etl=None
                seen[normalized.task.id]=[tc_message, task, artifacts]

            output.append(normalized)
        except Exception, e:
            Log.warning("problem", cause=e)

    keys = destination.extend({"id": etl2key(t.etl), "value": t} for t in output)
    return keys


def _normalize(tc_message, task):
    output = Dict()
    set_default(task, tc_message.status)

    output.task.id = task.taskId
    output.task.created = Date(task.created)
    output.task.deadline = Date(task.deadline)
    output.task.dependencies = unwraplist(task.dependencies)
    output.task.env = task.payload.env
    output.task.expires = Date(task.expires)
    output.task.priority = task.priority
    output.task.privisioner.id = task.provisionerId
    output.task.retries.remaining = task.retriesLeft
    output.task.retries.total = task.retries
    output.task.routes = task.routes
    output.task.runs = map(_normalize_run, task.runs)
    output.task.scheduler.id = task.schedulerId
    output.task.scopes = task.scopes
    output.task.state = task.state
    output.task.group.id = task.taskGroupId
    output.task.worker.type = task.workerType
    output.task.version = tc_message.version
    output.task.worker.group = tc_message.workerGroup
    output.task.worker.id = tc_message.workerId

    output.task.run = _normalize_run(task.runs[tc_message.runId])
    if output.task.run.id != tc_message.runId:
        Log.error("not expected")

    output.run = get_run_info(task)
    output.build = get_build_info(task)

    output.task.artifacts = unwraplist(_object_to_array(task.payload.artifacts, "name"))
    output.task.cache = unwraplist(_object_to_array(task.payload.cache, "name", "path"))
    output.task.command = " ".join(map(convert.string2quote, map(unicode.strip, task.payload.command)))
    output.task.tags = unwraplist(_object_to_array(task.tags, "name") + _object_to_array(task.metatdata, "name") + [{"name":k, "value":v} for k,v in task.extra.leaves()])

    if isinstance(task.payload.image, basestring):
        output.task.image = {"path": task.payload.image}

    return output


def _normalize_run(run):
    output = Dict()
    output.reason_created = run.reasonCreated
    output.id = run.runId
    output.scheduled = Date(run.scheduled)
    output.started = Date(run.started)
    output.state = run.state
    output.deadline = Date(run.takenUntil)
    output.worker.group = run.workerGroup
    output.worker.id = run.workerId
    return output



def get_run_info(task):
    """
    Get the run object that contains properties that describe the run of this job
    :param task: The task definition
    :return: The run object
    """
    run = Dict()
    run.machine = task.extra.treeherder.machine
    run.suite = task.extra.suite
    run.chunk = task.extra.chunks.current
    return run


def get_build_info(task):
    """
    Get a build object that describes the build
    :param task: The task definition
    :return: The build object
    """
    build = Dict()
    build.platform = task.extra.treeherder.build.platform

    # head_repo will look like "https://hg.mozilla.org/try/"
    head_repo = task.payload.env.GECKO_HEAD_REPOSITORY
    branch = head_repo.split("/")[-2]
    build.branch = branch

    build.revision = task.payload.env.GECKO_HEAD_REV
    build.revision12 = build.revision[0:12]

    # MOZILLA_BUILD_URL looks like this:
    # "https://queue.taskcluster.net/v1/task/e6TfNRfiR3W7ZbGS6SRGWg/artifacts/public/build/target.tar.bz2"
    build.url = task.payload.env.MOZILLA_BUILD_URL
    build.name = task.extra.build_name
    build.product = task.extra.build_product
    build.type = {"dbg": "debug"}.get(task.extra.build_type, task.extra.build_type)
    build.created_timestamp = Date(task.created)

    return build


def _object_to_array(value, key_name, value_name=None):
    try:
        if value_name==None:
            return [set_default(v, {key_name: k}) for k, v in value.items()]
        else:
            return [{key_name: k, value_name: v} for k, v in value.items()]
    except Exception, e:
        Log.error("unexpected", cause=e)

def _scrub(doc):
    if isinstance(doc, Mapping):
        for k, v in doc.items():
            doc[k] = _scrub(v)
    elif isinstance(doc, list):
        for i, v in enumerate(doc):
            doc[i] = _scrub(v)
    elif isinstance(doc, basestring):
        try:
            return Date(doc)
        except Exception, e:
            pass

    return doc
