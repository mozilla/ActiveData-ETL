# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Tyler Blair (tblair@cs.dal.ca)
#
from __future__ import division
from __future__ import unicode_literals

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.strings import expand_template

from StringIO import StringIO
import tempfile
import zipfile
import os

ACTIVE_DATA_QUERY = "https://activedata.allizom.org/query"
STATUS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
LIST_TASK_GROUP = "https://queue.taskcluster.net/v1/task-group/{{group_id}}/list"
RETRY = {"times": 3, "sleep": 5}

def process(source_key, source, destination, resources, please_stop=None):
    """
    This transform will turn a pulse message containing info about a gcov artifact (gcda or gcno file) on taskcluster
    into a list of records of method coverages. Each record represents a method in a source file, given a test.

    :param source_key: The key of the file containing the pulse messages in the source pulse message bucket
    :param source: The source pulse messages, in a batch of (usually) 100
    :param destination: The destination for the transformed data
    :param resources: not used
    :param please_stop: The stop signal to stop the current thread
    :return: The list of keys of files in the destination bucket
    """
    keys = []

    for msg_line_index, msg_line in enumerate(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        try:
            pulse_record = convert.json2value(msg_line)
        except Exception, e:
            if "JSON string is only whitespace" in e:
                continue
            else:
                Log.error("unexpected JSON decoding problem", cause=e)

        run_id = pulse_record.runId # TEMPORARY: UNTIL WE HOOK THIS UP TO THE PARSED TC RECORDS
        task_id = pulse_record.status.taskId
        task_group_id = pulse_record.status.taskGroupId

        # TEMPORARY: UNTIL WE HOOK THIS UP TO THE PARSED TC RECORDS
        artifacts = http.get_json(expand_template(ARTIFACTS_URL, {"task_id": task_id}), retry=RETRY)

        for artifact in artifacts.artifacts:
            if "gcda" in artifact.name:
                process_gcda_artifact(run_id, task_id, task_group_id, artifact)

    return keys


def process_gcda_artifact(run_id, task_id, task_group_id, artifact):
    Log.note("Processing gcda artifact {{artifact}}", artifact=artifact.name)

    tmpdir = tempfile.mkdtemp()
    os.mkdir('%s/ccov' % tmpdir)
    os.mkdir('%s/out' % tmpdir)

    Log.note('Using temp dir: {{tempdir}}', tempdir=tmpdir)

    # Download the gcda artifact
    # TEMPORARY: UNTIL WE HOOK THIS UP TO THE PARSED TC RECORDS
    gcda_full_artifact_url = 'https://public-artifacts.taskcluster.net/%s/%s/%s' % (task_id, run_id, artifact.name)

    Log.note('Fetching gcda artifact: {{url}}', url=gcda_full_artifact_url)

    zipdata = StringIO()
    zipdata.write(http.get(gcda_full_artifact_url).content)

    Log.note('Extracting gcda files to %s/ccov' % tmpdir)

    gcda_zipfile = zipfile.ZipFile(zipdata)
    gcda_zipfile.extractall('%s/ccov' % tmpdir)

    artifacts = group_to_gcno_artifact_urls(task_group_id)
    files = artifacts

    for file_url in files:
        # TODO delete old gcno files

        Log.note('Downloading gcno artifact {{file}}', file=file_url)

        zipdata = StringIO()
        zipdata.write(http.get(file_url).content)

        Log.note('Extracting gcno files to %s/ccov' % tmpdir)

        gcno_zipfile = zipfile.ZipFile(zipdata)
        gcno_zipfile.extractall('%s/ccov' % tmpdir)

        # TODO: Run LCOV

def group_to_gcno_artifact_urls(group_id):
    """
    Finds a task id in a task group with a given artifact.

    :param group_id:
    :param artifact_file_name:
    :return: task json object for the found task. None if no task was found.
    """

    result = http.post_json(ACTIVE_DATA_QUERY, json={
        "from": "task.task.artifacts",
        "where": {"and": [
            {"eq": {"task.group.id": group_id}},
            {"regex": {"name": ".*gcno.*"}}
        ]},
        "limit": 100,
        "select": ["url"]
    })

    return result.data.url # TODO This is a bit rough for now.
