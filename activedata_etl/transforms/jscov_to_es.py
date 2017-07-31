# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Trung Do (chin.bimbo@gmail.com)
#
from __future__ import division
from __future__ import unicode_literals

from activedata_etl import etl2key
from mo_dots import wrap, unwraplist, set_default
from mo_json import stream
from mo_logs import Log, machine_metadata

from mo_times.dates import Date
from mo_times.timer import Timer
from pyLibrary.env import http

STATUS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "https://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
RETRY = {"times": 3, "sleep": 5}


def process_jscov_artifact(source_key, resources, destination, task_cluster_record, artifact, artifact_etl, please_stop):
    # fetch the artifact
    response_stream = http.get(artifact.url).raw
    records = []
    with Timer("Processing {{ccov_file}}", param={"ccov_file": artifact.url}):
        def count_generator():
            count = 0
            while True:
                yield count
                count += 1

        counter = count_generator().next

        for source_file_index, obj in enumerate(stream.parse(response_stream, [], ["."])):
            if please_stop:
                Log.error("Shutdown detected. Stopping job ETL.")

            if source_file_index == 0:
                # this is not a jscov object but an object containing the version metadata
                # TODO: this metadata should not be here
                # TODO: this version info is not used right now. Make use of it later.
                jscov_format_version = obj.get("version")
                continue

            try:
                process_source_file(
                    artifact_etl,
                    counter,
                    obj,
                    task_cluster_record,
                    records
                )
            except Exception as e:
                Log.warning(
                    "Error processing test {{test_url}} and source file {{source}} while processing {{key}}",
                    key=source_key,
                    test_url=obj.get("testUrl"),
                    source=obj.get("sourceFile"),
                    cause=e
                )
    with Timer("writing {{num}} records to s3 for key {{key}}", param={"num": len(records), "key": source_key}):
        keys = destination.extend(records, overwrite=True)
    return keys


def process_source_file(parent_etl, count, obj, task_cluster_record, records):
    obj = wrap(obj)

    # get the test name. Just use the test file name at the moment
    # TODO: change this when needed
    try:
        test_name = unwraplist(obj.testUrl).split("/")[-1]
    except Exception as e:
        raise Log.error("can not get testUrl from coverage object", cause=e)

    # turn obj.covered (a list) into a set for use later
    file_covered = set(obj.covered)

    # file-level info
    file_info = wrap({
        "name": obj.sourceFile,
        "covered": sorted(obj.covered),
        "uncovered": sorted(obj.uncovered),
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
                    "language": "js",
                    "file": file_info,
                    "method": {
                        "name": method_name,
                        "covered": sorted(method_covered),
                        "uncovered": sorted(method_uncovered),
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
                "language": "js",
                "method": {
                    "covered": sorted(orphan_covered),
                    "uncovered": sorted(orphan_uncovered),
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
