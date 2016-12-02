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
from zipfile import ZipFile, BadZipfile
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile, mkdtemp

from activedata_etl import etl2key
from activedata_etl.parse_lcov import parse_lcov_coverage
from activedata_etl.transforms import EtlHeadGenerator
from pyLibrary import convert
from pyLibrary.debugs.logs import Log, machine_metadata
from pyLibrary.dot import wrap, Null, unwraplist, set_default
from pyLibrary.env import http
from pyLibrary.env.files import File
from pyLibrary.maths.randoms import Random
from pyLibrary.thread.multiprocess import Process
from pyLibrary.thread.threads import Thread, Queue, Lock
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer

ACTIVE_DATA_QUERY = "https://activedata.allizom.org/query"
RETRY = {"times": 3, "sleep": 5}
DEBUG = True
ENABLE_LCOV = True
WINDOWS_TEMP_DIR = "c:/msys64/tmp"
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
    keys = []
    etl_header_gen = EtlHeadGenerator(source_key)

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

        Log.note("{{id}}: {{num}} artifacts", id=task_cluster_record.task.id, num=len(artifacts))

        try: # TODO rm
            for artifact in artifacts:
                Log.note("{{name}}", name=artifact.name)
                if artifact.name.find("gcda") != -1:
                    keys.extend(process_gcda_artifact(source_key, resources, destination, etl_header_gen, task_cluster_record, artifact))
        except Exception as e:
            import traceback
            Log.note(traceback.format_exc())

    return keys


def process_gcda_artifact(source_key, resources, destination, etl_header_gen, task_cluster_record, gcda_artifact):
    """
    Processes a gcda artifact by downloading any gcno files for it and running lcov on them individually.
    The lcov results are then processed and converted to the standard ccov format.
    TODO this needs to coordinate new ccov json files to add to the s3 bucket. Return?
    """
    keys = []
    Log.note("Processing gcda artifact {{artifact}}", artifact=gcda_artifact.name)

    if os.name == "nt":
        tmpdir = WINDOWS_TEMP_DIR + "/" + Random.hex(10)
    else:
        tmpdir = mkdtemp()
    Log.note('Using temp dir: {{dir}}', dir=tmpdir)

    ccov = File(tmpdir + '/ccov')
    ccov.delete()
    out = File(tmpdir + "/out")
    out.delete()

    try:
        Log.note('Fetching gcda artifact: {{url}}', url=gcda_artifact.url)
        gcda_file = download_file(gcda_artifact.url)

        Log.note('Extracting gcda files to {{dir}}/ccov', dir=tmpdir)
        ZipFile(gcda_file).extractall('%s/ccov' % tmpdir)
    except BadZipfile:
        Log.note('Bad zip file for gcda artifact: {{url}}', url=gcda_artifact.url)
        return []

    parent_etl = task_cluster_record.etl
    artifacts = group_to_gcno_artifacts(task_cluster_record.task.group.id)
    files = artifacts

    # chop some not-needed, and verbose, properties from tc record
    task_cluster_record.etl = None
    task_cluster_record.action.timings = None
    task_cluster_record.action.etl = None
    task_cluster_record.task.artifacts = None
    task_cluster_record.task.runs = None

    records = []

    for file_obj in files:
        remove_files_recursively('%s/ccov' % tmpdir, 'gcno')

        Log.note('Downloading gcno artifact {{file}}', file=file_obj.url)

        _, file_etl = etl_header_gen.next(source_etl=parent_etl, url=gcda_artifact.url)

        etl_key = etl2key(file_etl)
        keys.append(etl_key)
        Log.note('GCNO records will be attached to etl_key: {{etl_key}}', etl_key=etl_key)

        gcno_file = download_file(file_obj.url)

        Log.note('Extracting gcno files to {{dir}}/ccov', dir=tmpdir)
        ZipFile(gcno_file).extractall('%s/ccov' % tmpdir)

        with Timer("Processing LCOV directory {{lcov_directory}}", param={"lcov_directory": '%s/ccov' % tmpdir}):
            lcov_coverage = run_lcov_on_directory('%s/ccov' % tmpdir)

            Log.note('Extracted {{num_records}} records', num_records=len(lcov_coverage))

            def count_generator():
                count = 0
                while True:
                    yield count
                    count += 1
            counter = count_generator().next

            for index, obj in enumerate(lcov_coverage):
                if index != 0:
                    process_source_file(file_etl, counter, obj, task_cluster_record, records)

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
    :return: queue with files
    """
    if os.name == 'nt':
        directory = File(directory_path)
        output = Queue("lcov artifacts")
        children = directory.children
        locker = Lock()
        expected = [len(children)]
        for subdir in children:
            filename = "output." + subdir.name + ".txt"
            linux_source_dir = subdir.abspath.replace(WINDOWS_TEMP_DIR, MSYS2_TEMP_DIR)
            windows_dest_file = File.new_instance(directory, filename)
            linux_dest_file = windows_dest_file.abspath.replace(WINDOWS_TEMP_DIR, MSYS2_TEMP_DIR)

            env = os.environ.copy()
            env[b"WD"] = b"C:\\msys64\\usr\\bin\\"
            env[b"MSYSTEM"] = b"MINGW64"

            proc = Process(
                "lcov: " + linux_dest_file,
                [
                    # "start",
                    # "/W",
                    "c:\\msys64\\usr\\bin\\mintty",
                    # "-i",
                    # "/msys2.ico",
                    "/usr/bin/bash",
                    "--login",
                    "-c",
                    # "C:\msys64\msys2_shell.cmd",
                    # "-mingw64",
                    # "-c",
                    "lcov --capture --directory " + linux_source_dir + " --output-file " + linux_dest_file + " 2>/dev/null"
                ],
                cwd="C:\\msys64",
                env=env
                # shell=True
            ) if ENABLE_LCOV else Null

            def closure_wrap(_dest_file, _proc):
                def is_done():
                    # PROCESS APPEARS TO STOP, BUT IT IS STILL RUNNING
                    # POLL THE FILE UNTIL IT STOPS CHANGING
                    while not _dest_file.exists:
                        Thread.sleep(seconds=1)
                    while True:
                        expiry = _dest_file.timestamp + 60
                        now = Date.now().unix
                        if now >= expiry:
                            break
                        Thread.sleep(seconds=expiry - now)

                    output.add(_dest_file)
                    with locker:
                        expected[0] -= 1
                        Log.note("{{dir}} is done.  REMAINING {{num}}", dir=_dest_file.name, num=expected[0])
                        if not expected[0]:
                            output.add(Thread.STOP)
                Log.note("added proc {{name}} for dir {{dir}}", name=_proc.name, dir=_dest_file.name)
                _proc.service_stopped.on_go(is_done)
            closure_wrap(windows_dest_file, proc)

        return output
    else:
        fdevnull = open(os.devnull, 'w')

        proc = Popen(['lcov', '--capture', '--directory', directory_path, '--output-file', '-'], stdout=PIPE, stderr=fdevnull)
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


def process_source_file(parent_etl, count, obj, task_cluster_record, records):
    obj = wrap(obj)

    # get the test name. Just use the test file name at the moment
    # TODO: change this when needed
    try:
        test_name = unwraplist(obj.testUrl).split("/")[-1]
    except Exception, e:
        raise Log.error("can not get testUrl from coverage object", cause=e)

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
    for method_name, method_lines in obj.methods.iteritems():
        all_method_lines = set(method_lines)
        method_covered = all_method_lines & file_covered
        method_uncovered = all_method_lines - method_covered
        method_percentage_covered = len(method_covered) / len(all_method_lines)

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
                    "id": count(),
                    "source": parent_etl,
                    "type": "join",
                    "machine": machine_metadata,
                    "timestamp": Date.now()
                }
            },
            task_cluster_record
        )
        key = etl2key(new_record.etl)
        records.append({"id": key, "value": new_record})

    # a record for all the lines that are not in any method
    # every file gets one because we can use it as canonical representative
    new_record = set_default(
        {
            "test": {
                "name": test_name,
                "url": obj.testUrl
            },
            "source": {
                "is_file": True,  # THE ORPHAN LINES WILL REPRESENT THE FILE AS A WHOLE
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
                "id": count(),
                "source": parent_etl,
                "type": "join",
                "machine": machine_metadata,
                "timestamp": Date.now()
            },
        },
        task_cluster_record
    )
    key = etl2key(new_record.etl)
    records.append({"id": key, "value": new_record})


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


