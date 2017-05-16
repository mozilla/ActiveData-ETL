# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Tyler Blair (tblair@cs.dal.ca)

from __future__ import division
from __future__ import unicode_literals

from subprocess import Popen, PIPE
from zipfile import ZipFile

import os
from activedata_etl import etl2key
from mo_dots import set_default
from mo_files import File, TempDirectory
from mo_json import json2value, value2json
from mo_logs import Log, machine_metadata
from mo_threads import Process, Till
from mo_times import Timer, Date

from activedata_etl.imports.parse_lcov import parse_lcov_coverage
from pyLibrary.env import http

ACTIVE_DATA_QUERY = "https://activedata.allizom.org/query"
RETRY = {"times": 3, "sleep": 5}
DEBUG = True
DEBUG_LCOV_FILE = None
KNOWN_SOURCES = ["/home/worker/workspace/build/src/"]


def process_gcda_artifact(source_key, resources, destination, gcda_artifact, task_cluster_record, artifact_etl, please_stop):
    """
    Processes a gcda artifact by downloading any gcno files for it and running lcov on them individually.
    The lcov results are then processed and converted to the standard ccov format.
    TODO this needs to coordinate new ccov json files to add to the s3 bucket. Return?
    """
    # Second part of CCOV transformation from SQS
    # gcda_artifact will be the URL to the gcda file
    if DEBUG:
        Log.note("Processing gcda artifact {{artifact}}", artifact=gcda_artifact.name)

    with TempDirectory() as tmpdir:
        Log.note('Using temp dir: {{dir}}', dir=tmpdir)
        gcda_file = File.new_instance(tmpdir, "gcda.zip").abspath
        gcno_file = File.new_instance(tmpdir, "gcno.zip").abspath
        dest_dir = File.new_instance(tmpdir, "ccov").abspath

        try:
            Log.note('Fetching gcda artifact: {{url}}', url=gcda_artifact.url)
            download_file(gcda_artifact.url, gcda_file)
            Log.note('Extracting gcda files to {{dir}}', dir=dest_dir)
            ZipFile(gcda_file).extractall(dest_dir)
        except Exception as e:
            Log.error('Problem with gcda artifact: {{url}}', url=gcda_artifact.url, cause=e)
            return []

        gcno_artifact = group_to_gcno_artifacts(task_cluster_record.task.group.id)
        try:
            Log.note('Downloading gcno artifact {{file}}', file=gcno_artifact.url)
            download_file(gcno_artifact.url, gcno_file)
            Log.note('Extracting gcno files to {{dir}}', dir=dest_dir)
            ZipFile(gcno_file).extractall(dest_dir)
        except Exception as e:
            Log.error('Problem with gcno artifact: {{url}} for key {{key}}', key=source_key, url=gcno_artifact.url, cause=e)
            return []

        # where actual transform is performed and written to S3
        process_directory(source_key, dest_dir, destination, task_cluster_record, artifact_etl, please_stop)
        etl_key = etl2key(artifact_etl)
        keys = [etl_key]
        return keys


def process_directory(source_key, source_dir, destination, task_cluster_record, file_etl, please_stop):

    try:
        file_map = {}
        linked_files = File.new_instance(source_dir, "linked-files-map.json")
        if linked_files:
            data = linked_files.read_json(flexible=False, leaves=False)
            for k, v in data.items():
                name = k.split("/")[-1]
                options = file_map.setdefault(name, [])
                options.append((k, v))
    except Exception as e:
        Log.warning("Missing linked-files-map.json for key {{key}}", key=source_key, cause=e)
        file_map = {}

    file_id = etl2key(file_etl)
    new_record = set_default(
        {
            "test": {
                "suite": task_cluster_record.run.suite.name,
                "chunk": task_cluster_record.run.chunk
            },
            "etl": {
                "source": file_etl,
                "type": "join",
                "machine": machine_metadata,
                "timestamp": Date.now()
            }
        },
        task_cluster_record
    )

    with Timer("Processing LCOV directory {{lcov_directory}}", param={"lcov_directory": source_dir}):
        def generator():
            known = set()
            count = 0
            map_used = 0
            prefix_used = 0

            if DEBUG_LCOV_FILE:
                lcov_coverage = list(parse_lcov_coverage(DEBUG_LCOV_FILE.read_lines()))
            elif os.name == 'nt':
                # grcov DOES NOT SUPPORT WINDOWS YET
                while not please_stop:
                    try:
                        lcov_coverage = list(run_lcov_on_windows(source_dir))  # TODO: Remove list()
                        break
                    except Exception as e:
                        if "Could not remove file" in e:
                            continue
                        raise e
            else:
                lcov_coverage = run_grcov(source_dir)

            for source in lcov_coverage:
                old_name = source.file.name
                short_name = old_name.split("/")[-1]
                candidates = file_map.get(short_name)
                if candidates:
                    new_name = best_suffix(old_name, candidates)
                    if new_name:
                        source.file.name = new_name
                        map_used += 1
                else:
                    for s in KNOWN_SOURCES:
                        if old_name.startswith(s):
                            if DEBUG:
                                Log.note("Not found in map: {{path}}", path=old_name)
                            source.file.name = old_name[len(s):]
                            prefix_used +=1
                            break
                    else:
                        if old_name not in known:
                            known.add(old_name)
                            Log.note("Can not translate {{path}}", path=old_name)
                new_record.source = source
                new_record.etl.id = count
                new_record._id = file_id+"."+unicode(count)

                count += 1
                yield value2json(new_record)
            if not map_used and file_map:
                Log.warning("file map not used while processing {{key}}", key=source_key)
            else:
                Log.note("file_map used {{amount|percent}} of {{num}}", amount=map_used/count, num=count)

        destination.write_lines(file_id, generator())


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
        Log.error("Got {{num}} gcno artifacts for task group {{group}}, not expected", num=len(result.data), group=group_id)
    return result.data[0]


def run_grcov(directory_path):
    """
    :param directory_path:
    :return: generator of coverage docs
    """
    with open(os.devnull, 'w') as fdevnull:
        proc = Popen(['./grcov', directory_path], stdout=PIPE, stderr=fdevnull)
        for json_str in proc.stdout:
            yield json2value(json_str.decode("utf8"))


def download_file(url, destination):
    tempfile = file(destination, "w+b")
    stream = http.get(url).raw
    try:
        for b in iter(lambda: stream.read(8192), b""):
            tempfile.write(b)
    finally:
        stream.close()


def run_lcov_on_windows(directory_path):
    WINDOWS_TEMP_DIR = "c:/msys64/tmp/ccov"
    MSYS2_TEMP_DIR = "/tmp/ccov"

    File(WINDOWS_TEMP_DIR).delete()
    windows_dest_dir = File.new_instance(WINDOWS_TEMP_DIR, File(directory_path).name)
    File.copy(directory_path, windows_dest_dir)

    # directory = File(directory_path)
    filename = "output.txt"
    linux_source_dir = windows_dest_dir.abspath.lower().replace(WINDOWS_TEMP_DIR, MSYS2_TEMP_DIR)
    windows_dest_file = File.new_instance(WINDOWS_TEMP_DIR, filename).delete()
    linux_dest_file = windows_dest_file.abspath.lower().replace(WINDOWS_TEMP_DIR, MSYS2_TEMP_DIR)

    env = os.environ.copy()
    env[b"WD"] = b"C:\\msys64\\usr\\bin\\"
    env[b"MSYSTEM"] = b"MINGW64"

    proc = Process(
        "lcov: " + linux_dest_file,
        [
            "c:\\msys64\\usr\\bin\\mintty",
            "/usr/bin/bash",
            "--login",
            "-c",
            "lcov --capture --directory " + linux_source_dir + " --output-file " + linux_dest_file + " 2>/dev/null"
            # "./grcov " + linux_source_dir + " >" + linux_dest_file + " 2>/dev/null"
        ],
        cwd="C:\\msys64",
        env=env
        # shell=True
    )

    # PROCESS APPEARS TO STOP, BUT IT IS STILL RUNNING
    # POLL THE FILE UNTIL IT STOPS CHANGING
    proc.service_stopped.wait()
    while not windows_dest_file.exists:
        Till(seconds=1).wait()
    while True:
        expiry = windows_dest_file.timestamp + 20  # assume done after 20seconds of inactivity
        now = Date.now().unix
        if now >= expiry:
            break
        Till(till=expiry).wait()

    for cov in parse_lcov_coverage(open(windows_dest_file.abspath, "rb")):
        yield cov


def best_suffix(a, candidates):
    best = None
    best_len = 0
    for c, n in candidates:
        c_len = len(os.path.commonprefix([a[::-1], c[::-1]]))
        if c_len > best_len:
            if c == a:
                Log.note("Perfect match!")
                return n
            best = n
    return best
