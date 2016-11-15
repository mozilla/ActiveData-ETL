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
from mmap import mmap
from subprocess import Popen, PIPE

import taskcluster
from pandas.io.wb import download

from mohg.repos.changesets import Changeset
from mohg.repos.revisions import Revision
from pyLibrary import convert
from pyLibrary.debugs.logs import Log, machine_metadata
from pyLibrary.dot import wrap, Dict, unwraplist
from pyLibrary.env import http
from pyLibrary.jsons import stream
from pyLibrary.strings import expand_template
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer
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
            try:
                Log.note("{{name}}", name=artifact.name)
                if artifact.name.find("gcda") != -1:
                    keys.extend(process_gcda_artifact(source_key, destination, etl_header_gen, task_cluster_record, artifact))
            except Exception as e:
                Log.warning("problem processing {{artifact}}", artifact=artifact.name, cause=e)

    return keys


def process_gcda_artifact(source_key, destination, etl_header_gen, task_cluster_record, artifact):
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

    zipdata = download_file(artifact.url)

    try:
        gcda_zipfile = zipfile.ZipFile(zipdata)

        Log.note('Extracting gcda files to {{dir}}/ccov', dir=tmpdir)

        gcda_zipfile.extractall('%s/ccov' % tmpdir)
    except zipfile.BadZipfile:
        Log.note('Bad zip file for gcda artifact: {{url}}', url=artifact.url)
        return []

    artifacts = group_to_gcno_artifacts(task_cluster_record.task.group.id)
    files = artifacts

    records = []

    for file_obj in files:
        remove_files_recursively('%s/ccov' % tmpdir, 'gcno')

        Log.note('Downloading gcno artifact {{file}}', file=file_obj.url)

        _, dest_etl = etl_header_gen.next(task_cluster_record.etl, url=file_obj.url)

        etl_key = etl2key(dest_etl)
        keys.append(etl_key)
        Log.note('GCNO records will be attached to etl_key: {{etl_key}}', etl_key=etl_key)

        zipdata = StringIO()
        zipdata.write(http.get(file_obj.url).content)

        Log.note('Extracting gcno files to {{dir}}/ccov', dir=tmpdir)

        gcno_zipfile = zipfile.ZipFile(zipdata)
        gcno_zipfile.extractall('%s/ccov' % tmpdir)

        Log.note('Running LCOV on ccov directory')

        lcov_coverage = run_lcov_on_directory('%s/ccov' % tmpdir)

        Log.note('Extracted {{num_records}} records', num_records=len(lcov_coverage))

        process_source_file(dest_etl, lcov_coverage, task_cluster_record, records)

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

    proc = Popen(['lcov', '--capture', '--directory', directory_path, '--output-file', '-'], stdout=PIPE, stderr=PIPE)
    results = parse_lcov_coverage(proc.stdout)

    return results


def process_source_file(dest_etl, obj, task_cluster_record, records):
    obj = wrap(obj)

    # get the test name. Just use the test file name at the moment
    # TODO: change this when needed
    try:
        test_name = unwraplist(obj.testUrl).split("/")[-1]
    except Exception, e:
        Log.error("can not get testUrl from coverage object", cause=e)

    # turn obj.covered (a list) into a set for use later
    file_covered = set(obj.covered)

    # file-level info
    file_info = wrap({
        "name": obj.sourceFile,
        "covered": [{"line": c} for c in obj.covered],
        "uncovered": obj.uncovered,
        "total_covered": len(obj.covered),
        "total_uncovered": len(obj.uncovered),
        "percentage_covered": len(obj.covered) / (len(obj.covered) + len(obj.uncovered))
    })

    # orphan lines (i.e. lines without a method), initialized to all lines
    orphan_covered = set(obj.covered)
    orphan_uncovered = set(obj.uncovered)

    # iterate through the methods of this source file
    # a variable to count the number of lines so far for this source file
    for count, (method_name, method_lines) in enumerate(obj.methods.iteritems()):
        all_method_lines_set = set(method_lines)
        method_covered = all_method_lines_set & file_covered
        method_uncovered = all_method_lines_set - method_covered
        method_percentage_covered = len(method_covered) / len(all_method_lines_set)

        orphan_covered = orphan_covered - method_covered
        orphan_uncovered = orphan_uncovered - method_uncovered

        new_record = set_default(
            {
                "test": {
                    "name": test_name,
                    "url": obj.testUrl
                },
                "source": {
                    "file": file_info,
                    "method": {
                        "name": method_name,
                        "covered": [{"line": c} for c in method_covered],
                        "uncovered": method_uncovered,
                        "total_covered": len(method_covered),
                        "total_uncovered": len(method_uncovered),
                        "percentage_covered": method_percentage_covered,
                    }
                },
                "etl": {
                    "id": count+1,
                    "source": dest_etl,
                    "type": "join",
                    "machine": machine_metadata,
                    "timestamp": Date.now()
                }
            },
            task_cluster_record
        )
        records.append({"id": etl2key(new_record.etl), "value": new_record})

    # a record for all the lines that are not in any method
    # every file gets one because we can use it as canonical representative
    new_record = wrap({
        "test": {
            "name": test_name,
            "url": obj.testUrl
        },
        "source": {
            "is_file": True,  # THE ORPHAN LINES WILl REPRESENT THE FILE AS A WHILE
            "file": file_info,
            "method": {
                "covered": [{"line": c} for c in orphan_covered],
                "uncovered": orphan_uncovered,
                "total_covered": len(orphan_covered),
                "total_uncovered": len(orphan_uncovered),
                "percentage_covered": len(orphan_covered) / max(1, (len(orphan_covered) + len(orphan_uncovered))),
            }
        },
        "etl": {
            "id": 0,
            "source": dest_etl,
            "type": "join",
            "machine": machine_metadata,
            "timestamp": Date.now()
        },
        "repo": repo,
        "run": run,
        "build": build,
        "is_file": True
    })
    records.append({"id": etl2key(new_record.etl), "value": new_record})


def download_file(url):
    output = tempfile.TemporaryFile()
    with open(output):
        destination = mmap(output.fileno())
        for b in http.get(url).iter_content():
            destination.write(b)
    return output
