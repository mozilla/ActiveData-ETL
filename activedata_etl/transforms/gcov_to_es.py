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
from activedata_etl.transforms import EtlHeadGenerator

STATUS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
RETRY = {"times": 3, "sleep": 5}

# task groupId -> taskId that has the gcno file
task_group_to_build_task_id_map = {}

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
    etl_header_gen = EtlHeadGenerator(source_key)
    ccov_artifact_count = 0

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

        task_id = pulse_record.status.taskId
        task_group_id = pulse_record.status.taskGroupId

        # TEMPORARY: UNTIL WE HOOK THIS UP TO THE PARSED TC RECORDS
        artifacts = http.get_json(expand_template(ARTIFACTS_URL, {"task_id": task_id}), retry=RETRY)

        for artifact in artifacts.artifacts:
            artifact_file_name = artifact.name

            if "gcno" in artifact_file_name:
                task_group_to_build_task_id_map[task_group_id] = task_id
            elif "gcda" in artifact_file_name:
                if task_group_id not in task_group_to_build_task_id_map:
                    Log.warning("Failed to find parent build task (for gcno) on task {{task_id}} and group {{group_id}}", task_id=task_id, group_id=task_group_id)
                    continue

                parent_build_task_id = task_group_to_build_task_id_map[task_group_id]

                Log.warning("Unimplemented gcda+gcno -> lcov! gcda={{gcda_task_id}} gcno={{gcno_task_id}} group={{group_id}}", gcda_task_id=task_id, gcno_task_id=parent_build_task_id, group_id=task_group_id)

    return keys

