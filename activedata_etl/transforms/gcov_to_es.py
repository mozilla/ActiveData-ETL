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
import shutil
import zipfile
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile, mkdtemp

from activedata_etl import etl2key
from activedata_etl.parse_lcov import parse_lcov_coverage
from activedata_etl.transforms import EtlHeadGenerator
from pyLibrary import convert
from pyLibrary.debugs.logs import Log, machine_metadata
from pyLibrary.dot import wrap, unwraplist, set_default
from pyLibrary.env import http
from pyLibrary.env.big_data import sbytes2ilines
from pyLibrary.env.files import File
from pyLibrary.thread.multiprocess import Process
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer

ACTIVE_DATA_QUERY = "https://activedata.allizom.org/query"
RETRY = {"times": 3, "sleep": 5}
DEBUG = True
ENABLE_LCOV = False
WINDOWS_TEMP_DIR = "c:\\msys64\\tmp"
MSYS2_TEMP_DIR = "/tmp"


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
            task_cluster_record.action.timings = None
            task_cluster_record.action.etl = None
            task_cluster_record.task.runs = None
            task_cluster_record.task.tags = None
            task_cluster_record.task.env = None
        except Exception, e:
            if "JSON string is only whitespace" in e:
                continue
            else:
                Log.error("unexpected JSON decoding problem", cause=e)

        artifacts, task_cluster_record.task.artifacts = task_cluster_record.task.artifacts, None

        # Log.note("{{id}}: {{num}} artifacts", id=task_cluster_record.task.id, num=len(artifacts))

        for artifact in artifacts:
            if artifact.name.find("gcda") != -1:
                try:
                    Log.note("Process GCDA artifact {{name}} for key {{key}}", name=artifact.name, key=task_cluster_record._id)
                    keys.extend(process_gcda_artifact(source_key, destination, etl_header_gen, task_cluster_record, artifact))
                except Exception as e:
                    Log.warning("problem processing {{artifact}}", artifact=artifact.name, cause=e)

    return keys


def process_gcda_artifact(source_key, destination, etl_header_gen, task_cluster_record, gcda_artifact):
    """
    Processes a gcda artifact by downloading any gcno files for it and running lcov on them individually.
    The lcov results are then processed and converted to the standard ccov format.
    TODO this needs to coordinate new ccov json files to add to the s3 bucket. Return?
    """
    Log.note("Processing gcda artifact {{artifact}}", artifact=gcda_artifact.name)

    if os.name == "nt":
        tmpdir = WINDOWS_TEMP_DIR
    else:
        tmpdir = mkdtemp()
    Log.note('Using temp dir: {{dir}}', dir=tmpdir)

    ccov = File(tmpdir + '/ccov')
    ccov.delete()
    out = File(tmpdir + "/out")
    out.delete()

    Log.note('Fetching gcda artifact: {{url}}', url=gcda_artifact.url)
    gcda_file = download_file(gcda_artifact.url)

    Log.note('Extracting gcda files to {{dir}}/ccov', dir=tmpdir)
    ZipFile(gcda_file).extractall('%s/ccov' % tmpdir)

    artifacts = group_to_gcno_artifacts(task_cluster_record.task.group.id)
    if len(artifacts) != 1:
        Log.error("Do not know how to handle more than one gcno file")
    gcno_artifact = artifacts[0]
    remove_files_recursively('%s/ccov' % tmpdir, 'gcno')

    Log.note('Downloading gcno artifact {{file}}', file=gcno_artifact.url)
    gcno_file = download_file(gcno_artifact.url)

    Log.note('Extracting gcno files to {{dir}}/ccov', dir=tmpdir)
    ZipFile(gcno_file).extractall('%s/ccov' % tmpdir)

    Log.note('Running LCOV on ccov directory')
    lcov_files = run_lcov_on_directory('%s/ccov' % tmpdir)

    keys = []

    for file in lcov_files:
        with file:
            records = parse_lcov_coverage(source_key, etl_header_gen, file)
            Log.note('Extracted {{num_records}} records', num_records=len(records))
            for r in records:
                r._id, etl = etl_header_gen.next(task_cluster_record.etl)
                r.etl.gcno = gcno_artifact.url
                r.etl.gcda = gcda_artifact.url
                set_default(r, task_cluster_record)
                r.etl = etl
                keys.append(r._id)

    remove_files_recursively('%s/ccov' % tmpdir, 'gcno')
    shutil.rmtree(tmpdir)

    with Timer("writing {{num}} records to s3", {"num": len(records)}):
        destination.extend(records, overwrite=True)

    return keys


def group_to_gcno_artifacts(group_id):
    """
    Finds a task id in a task group with a given artifact.

    :param group_id:
    :param artifact_file_name:
    :return: task json object for the found task. None if no task was found.
    """

    data = http.post_json(ACTIVE_DATA_QUERY, json={
        "from": "task.task.artifacts",
        "where": {"and": [
            {"eq": {"task.group.id": group_id}},
            {"regex": {"name": ".*gcno.*"}}
        ]},
        "limit": 100,
        "select": ["task.id", "url"]
    })

    values = data.data.values()

    results = []

    for i in range(len(values[0])):
        # Note: values is sensitive to select order
        # Currently bug in pyLibrary Dict and can't
        # retrieve the task.id member (TODO)
        results.append(wrap({
            'task_id': values[1][i],
            'url': values[0][i]
        }))

    return results


def run_lcov_on_directory(directory_path):
    """
    Runs lcov on a directory.
    :param directory_path:
    :return: array of parsed coverage artifacts (files)
    """
    if os.name == 'nt':
        directory = File(directory_path)
        procs = []
        for c in directory.children:
            subdir = MSYS2_TEMP_DIR + "/ccov/" + c.name
            filename = "output." + c.name + ".txt"
            fullpath = MSYS2_TEMP_DIR + "/" + filename

            procs.append((
                filename,
                Process(
                    "lcov"+unicode(len(procs)),
                    [
                        "C:\msys64\msys2_shell.cmd",
                        "-mingw64",
                        "-c",
                        "lcov --capture --directory " + subdir + " --output-file " + fullpath + " 2>/dev/null"
                    ]
                ) if ENABLE_LCOV else Null
            ))

        for n, p in procs:
            p.join()

        return [File(WINDOWS_TEMP_DIR + "/" + n) for n, p in procs]
    else:
        Log.error("must return a list of files, it returns a stream instead")
        proc = Popen(['lcov', '--capture', '--directory', directory_path, '--output-file', '-'], stdout=PIPE, stderr=PIPE)
        results = parse_lcov_coverage(proc.stdout)
        return results


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


