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
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile, mkdtemp
from zipfile import ZipFile, BadZipfile

from activedata_etl import etl2key
from activedata_etl.imports.task import minimize_task
from activedata_etl.parse_lcov import parse_lcov_coverage
from activedata_etl.transforms import EtlHeadGenerator, TRY_AGAIN_LATER
from mo_dots import set_default
from mo_files import File
from mo_json import json2value, value2json
from mo_logs import Log, machine_metadata
from mo_threads import Process
from mo_times import Timer, Date
from pyLibrary.env import http

ACTIVE_DATA_QUERY = "https://activedata.allizom.org/query"
RETRY = {"times": 3, "sleep": 5}
DEBUG = True


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
    keys = []
    for msg_line_index, msg_line in enumerate(list(source.read_lines())): #readline() for local
        # Enter once collected artifacts
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        try:
            task_cluster_record = json2value(msg_line)
        except Exception, e:
            if "JSON string is only whitespace" in e:
                continue
            else:
                Log.error("unexpected JSON decoding problem", cause=e)

        parent_etl = task_cluster_record.etl
        artifacts = task_cluster_record.task.artifacts

        if DEBUG:
            Log.note("{{id}}: {{num}} artifacts", id=task_cluster_record.task.id, num=len(artifacts))
        etl_header_gen = EtlHeadGenerator(source_key)
        for artifact in artifacts:
            try:
                if artifact.name.find("gcda") != -1:
                    Log.note("artifact {{name}}", name=artifact.name)
                    keys.extend(process_gcda_artifact(source_key, resources, destination, etl_header_gen, task_cluster_record, artifact, parent_etl))
            except Exception as e:
                Log.warning("grcov Failed to process {{artifact}}", artifact=artifact.url, cause=e)
    if not keys:
        return None
    else:
        return keys


def process_gcda_artifact(source_key, resources, destination, etl_header_gen, task_cluster_record, gcda_artifact, parent_etl):
    """
    Processes a gcda artifact by downloading any gcno files for it and running lcov on them individually.
    The lcov results are then processed and converted to the standard ccov format.
    TODO this needs to coordinate new ccov json files to add to the s3 bucket. Return?
    """
    Log.note("Processing gcda artifact {{artifact}}", artifact=gcda_artifact.name)

    tmpdir = mkdtemp()
    Log.note('Using temp dir: {{dir}}', dir=tmpdir)

    ccov = File(tmpdir + '/ccov').delete()
    out = File(tmpdir + "/out").delete()

    try:
        Log.note('Fetching gcda artifact: {{url}}', url=gcda_artifact.url)
        gcda_file = download_file(gcda_artifact.url)
        #gcda_file = 'tests/resources/ccov/code-coverage.zip'
        Log.note('Extracting gcda files to {{dir}}/ccov', dir=tmpdir)
        ZipFile(gcda_file).extractall('%s/ccov' % tmpdir)
    except BadZipfile:
        Log.note('Bad zip file for gcda artifact: {{url}}', url=gcda_artifact.url)
        return []

    file_obj = group_to_gcno_artifacts(task_cluster_record.task.group.id)

    remove_files_recursively('%s/ccov' % tmpdir, 'gcno')

    Log.note('Downloading gcno artifact {{file}}', file=file_obj.url)
    _, file_etl = etl_header_gen.next(source_etl=parent_etl, url=gcda_artifact.url)

    etl_key = etl2key(file_etl)
    Log.note('GCNO records will be attached to etl_key: {{etl_key}}', etl_key=etl_key)

    gcno_file = download_file(file_obj.url)

    Log.note('Extracting gcno files to {{dir}}/ccov', dir=tmpdir)
    ZipFile(gcno_file).extractall('%s/ccov' % tmpdir)

    minimize_task(task_cluster_record)
    process_directory('%s/ccov' % tmpdir, destination, task_cluster_record, file_etl)
    File(tmpdir).delete()

    keys = [etl_key]
    return keys


def process_directory(source_dir, destination, task_cluster_record, file_etl):
    new_record = set_default(
        {
            "test": {
                "suite": task_cluster_record.run.suite.name,
                "chunk": task_cluster_record.run.chunk
            },
            "source": "%PLACEHOLDER%",
            "etl": {
                "id": "%PLACEHOLDER_ID%",
                "source": file_etl,
                "type": "join",
                "machine": machine_metadata,
                "timestamp": Date.now()
            }
        },
        task_cluster_record
    )

    json_with_placeholders = value2json(new_record)

    with Timer("Processing LCOV directory {{lcov_directory}}", param={"lcov_directory": source_dir}):
        lcov_coverage = run_lcov_on_directory(source_dir)

        def generator():
            count = 0
            for json_str in lcov_coverage:
                res = json_with_placeholders.replace("\"%PLACEHOLDER%\"", json_str.decode('utf8').rstrip("\n"))
                res = res.replace("\"%PLACEHOLDER_ID%\"", unicode(count))
                count += 1
                if DEBUG:
                    try:
                        json2value(res)
                    except Exception as e:
                        Log.error("grcov did not result in JSON", cause=e)
                yield res

        destination.write_lines(etl2key(file_etl), generator())


def group_to_gcno_artifacts(group_id):
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
        "select": [{"name": "task_id", "value": "task.id"}, "url"],
        "format": "list"
    })

    if len(result.data) != 1:
        Log.error(TRY_AGAIN_LATER, reason="got " + unicode(len(result.data)) + " gcno artifacts for task group " + group_id + ", not expected")
    return result.data[0]


def run_lcov_on_directory(directory_path):
    """
    Runs lcov on a directory.
    :param directory_path:
    :return: queue with files
    """
    if os.name == 'nt':
        grcov = File("./resources/binaries/grcov.exe").abspath
        with Process("grcov:" +directory_path, [grcov, directory_path], env={b"RUST_BACKTRACE": b"full"}, debug=True) as proc:
            results = parse_lcov_coverage(proc.stdout)
        return results
    else:
        fdevnull = open(os.devnull, 'w')

        proc = Popen(['./grcov', directory_path], stdout=PIPE, stderr=fdevnull)
        return proc.stdout


def download_file(url):
    tempfile = NamedTemporaryFile(delete=False)
    stream = http.get(url).raw
    try:
        for b in iter(lambda: stream.read(8192), b""):
            tempfile.write(b)
    finally:
        stream.close()

    return tempfile


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

