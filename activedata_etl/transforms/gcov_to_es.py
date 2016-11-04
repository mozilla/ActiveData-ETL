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

import os
import tempfile
import shutil
import zipfile
from StringIO import StringIO
from subprocess import Popen, PIPE

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.strings import expand_template
from activedata_etl import etl2key
from activedata_etl.parse_lcov import parse_lcov_coverage
from activedata_etl.transforms import EtlHeadGenerator

ACTIVE_DATA_QUERY = "https://activedata.allizom.org/query"
STATUS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
LIST_TASK_GROUP = "https://queue.taskcluster.net/v1/task-group/{{group_id}}/list"
RETRY = {"times": 3, "sleep": 5}


def remove_files_recursively(root_directory, file_extension):
    """
    Removes files with the given file extension from a directory recursively.

    :param root_directory: The directory to remove files from recursively
    :param file_extension: The file extension files must match
    """
    full_ext = '.%s' % file_extension

    for root, dirs, files in os.walk(root_directory):
        for file in files:
            if file.endswith(full_ext):
                os.remove(os.path.join(root, file))


def process(source_key, source, destination, resources, please_stop=None):
    """
    This transform will turn a pulse message containing info about a gcov artifact (gcda or gcno file) on taskcluster
    into a list of records of method coverages. Each record represents a method in a source file, given a test.

    :param source_key: The key of the file containing the normalized task cluster messages
    :param source: The file contents, a cr-delimited list of normalized task cluster messages
    :param destination: The destination for the transformed data
    :param resources: not used
    :param please_stop: The stop signal to stop the current thread
    :return: The list of keys of files in the destination bucket
    """
    etl_header_gen = EtlHeadGenerator(source_key)
    keys = []

    for msg_line_index, msg_line in enumerate(list(source.read_lines())):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        try:
            task_cluster_record = convert.json2value(msg_line)
            # SCRUB PROPERTIES WE DO NOT WANT
            task_cluster_record.actions = None
            task_cluster_record.runs = None
            task_cluster_record.tags = None
        except Exception, e:
            if "JSON string is only whitespace" in e:
                continue
            else:
                Log.error("unexpected JSON decoding problem", cause=e)

        artifacts, task_cluster_record.task.artifacts = task_cluster_record.task.artifacts, None

        Log.note("{{id}}: {{num}} artifacts", id=task_cluster_record.task.id, num=len(artifacts))

        for artifact in artifacts:
            Log.note("{{name}}", name=artifact.name)
            if artifact.name.find("gcda") != -1:
                keys.extend(process_gcda_artifact(source_key, etl_header_gen, task_cluster_record, artifact))

    return keys


def process_gcda_artifact(source_key, etl_header_gen, task_cluster_record, artifact):
    """
    Processes a gcda artifact by downloading any gcno files for it and running lcov on them individually.
    The lcov results are then processed and converted to the standard ccov format.
    TODO this needs to coordinate new ccov json files to add to the s3 bucket. Return?
    """
    keys = []
    Log.note("Processing gcda artifact {{artifact}}", artifact=artifact.name)

    tmpdir = tempfile.mkdtemp()
    os.mkdir('%s/ccov' % tmpdir)
    os.mkdir('%s/out' % tmpdir)

    Log.note('Using temp dir: {{tempdir}}', tempdir=tmpdir)

    Log.note('Fetching gcda artifact: {{url}}', url=artifact.url)

    zipdata = StringIO()
    zipdata.write(http.get(artifact.url).content)

    Log.note('Extracting gcda files to {{dir}}/ccov', dir=tmpdir)

    gcda_zipfile = zipfile.ZipFile(zipdata)
    gcda_zipfile.extractall('%s/ccov' % tmpdir)

    artifacts = group_to_gcno_artifact_urls(task_cluster_record.task.group.id)
    files = artifacts

    for file_url in files:
        remove_files_recursively('%s/ccov' % tmpdir, 'gcno')

        Log.note('Downloading gcno artifact {{file}}', file=file_url)

        _, dest_etl = etl_header_gen.next(task_cluster_record.etl, url=file_url)
        add_tc_prefix(dest_etl)

        etl_key = etl2key(dest_etl)
        keys.append(etl_key)
        Log.note('GCNO records will be attached to etl_key: {{etl_key}}', etl_key=etl_key)

        zipdata = StringIO()
        zipdata.write(http.get(file_url).content)

        Log.note('Extracting gcno files to {{dir}}/ccov', dir=tmpdir)

        gcno_zipfile = zipfile.ZipFile(zipdata)
        gcno_zipfile.extractall('%s/ccov' % tmpdir)

        Log.note('Running LCOV on ccov directory')
 
        lcov_coverage = run_lcov_on_directory('%s/ccov' % tmpdir)

        Log.note('Extracted {{num_records}} records', num_records=len(lcov_coverage))

        remove_files_recursively('%s/ccov' % tmpdir, 'gcno')

    shutil.rmtree(tmpdir)
    return keys

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


def run_lcov_on_directory(directory_path):
    """
    Runs lcov on a directory.
    :param directory_path:
    :return: array of parsed coverage artifacts (files)
    """

    proc = Popen(['lcov', '--capture', '--directory', directory_path, '--output-file', '-'], stdout=PIPE, stderr=PIPE)
    results = parse_lcov_coverage(proc.stdout)

    return results


def add_tc_prefix(dest_etl):
    # FIX ONCE TC LOGGER IS USING "tc" PREFIX FOR KEYS
    if not dest_etl.source.source.source:
        dest_etl.source.source.type = "join"
        dest_etl.source.source.source = {"id": "tc"}
