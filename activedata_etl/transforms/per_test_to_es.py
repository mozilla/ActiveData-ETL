# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Marco Castelluccio (mcastelluccio@mozilla.com)
#
from __future__ import division
from __future__ import unicode_literals

from zipfile import ZipFile

from activedata_etl import etl2key
from activedata_etl.imports.coverage_util import download_file, LANGUAGE_MAPPINGS, tuid_batches
from mo_dots import wrap, set_default
from mo_files import TempFile
from mo_json import stream, value2json
from mo_logs import Log, machine_metadata
from mo_times.dates import Date
from mo_times.timer import Timer

ENABLE_METHOD_COVERAGE = False
FILE_TOO_LONG = 100*1000

urls_w_uncoverable_lines = set()


def process_per_test_artifact(source_key, resources, destination, task_cluster_record, artifact, artifact_etl, please_stop):

    def create_record(parent_etl, count, filename, covered, uncovered):
        file_details = resources.file_mapper.find(source_key, filename, artifact, task_cluster_record)

        coverable_line_count = len(covered) + len(uncovered)

        if not coverable_line_count and artifact.url not in urls_w_uncoverable_lines:
            urls_w_uncoverable_lines.add(artifact.url)
            Log.warning("per-test-coverage {{url}} has uncoverable lines", url=artifact.url)

        new_record = set_default(
            {
                "source": {
                    "language": [lang for lang, extensions in LANGUAGE_MAPPINGS if filename.endswith(extensions)],
                    "file": set_default(
                        file_details,
                        {
                            "covered": sorted(covered),
                            "uncovered": sorted(uncovered),
                            "total_covered": len(covered),
                            "total_uncovered": len(uncovered),
                            "percentage_covered": len(covered) / coverable_line_count if coverable_line_count else None
                        }
                    )
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

        return new_record

    def process_source_file(parent_etl, count, suite, test, sf):
        sf = wrap(sf)

        covered = []
        uncovered = []
        coverable = []
        for l, c in enumerate(sf["coverage"], start=1):  # FIRST LINE STARTS AT ONE
            if c is None:
                continue
            coverable.append(l)
            if c > 0:
                covered.append(l)
            else:
                uncovered.append(l)

        # turn covered into a set for use later
        file_covered = set(covered)
        coverable = set(coverable)

        if len(coverable) > FILE_TOO_LONG:
            return

        record = create_record(parent_etl, count, sf["name"], covered, uncovered)
        record.test = {
            "name": test,
            "suite": suite,
        }

        if ENABLE_METHOD_COVERAGE:
            # orphan lines (i.e. lines without a method), initialized to all lines
            orphan_covered = set(covered)
            orphan_uncovered = set(uncovered)

            # iterate through the methods of this source file
            # a variable to count the number of lines so far for this source file
            methods = sf['functions'] if 'functions' in sf else []
            method_start_indexes = [method['start'] for method in methods]
            end = len(sf["coverage"])
            for method in methods:
                func_start = method['start']
                func_end = end

                for start in method_start_indexes:
                    if start > func_start:
                        func_end = start
                        break

                method_lines = []
                for l in coverable:
                    if l < func_start:
                        continue

                    if l >= func_end:
                        break

                    method_lines.append(l)

                all_method_lines = set(method_lines)
                method_covered = all_method_lines & file_covered
                method_uncovered = all_method_lines - method_covered
                method_percentage_covered = len(method_covered) / len(all_method_lines) if len(all_method_lines) > 0 else None

                orphan_covered = orphan_covered - method_covered
                orphan_uncovered = orphan_uncovered - method_uncovered

                # Record method coverage info
                record.source.method = {
                    "name": method['name'],
                    "covered": sorted(method_covered),
                    "uncovered": sorted(method_uncovered),
                    "total_covered": len(method_covered),
                    "total_uncovered": len(method_uncovered),
                    "percentage_covered": method_percentage_covered,
                }

                # Timestamp this record
                record.etl.timestamp = Date.now()
                yield record

            # a record for all the lines that are not in any method
            # every file gets one because we can use it as canonical representative
            # Record method coverage info
            total_orphan_coverable = len(orphan_covered) + len(orphan_uncovered)
            record.source.method = {
                "covered": sorted(orphan_covered),
                "uncovered": sorted(orphan_uncovered),
                "total_covered": len(orphan_covered),
                "total_uncovered": len(orphan_uncovered),
                "percentage_covered": len(orphan_covered) / total_orphan_coverable if total_orphan_coverable else None
            }

            # Timestamp this record
            record.etl.timestamp = Date.now()
            yield record

        # Timestamp this record
        record.etl.timestamp = Date.now()
        record.source.is_file = True
        yield record

    def generator():
        with ZipFile(temp_file.abspath) as zipped:
            for zip_name in zipped.namelist():
                for record in stream.parse(zipped.open(zip_name), "report.source_files", {"report.source_files", "suite", "test"}):
                    if please_stop:
                        Log.error("Shutdown detected. Stopping job ETL.")

                    try:
                        for d in process_source_file(
                            artifact_etl,
                            counter,
                            record.suite,
                            record.test,
                            record.report.source_files
                        ):
                            yield d
                    except Exception as e:
                        Log.warning(
                            "Error processing test {{test}} while processing {{key}}",
                            key=source_key,
                            test=record.test,
                            cause=e
                        )

    counter = count_generator().next

    with TempFile() as temp_file:
        with Timer("Downloading {{url}}", param={"url": artifact.url}):
            download_file(artifact.url, temp_file.abspath)

        key = etl2key(artifact_etl)
        with Timer("Processing per-test reports for key {{key}}", param={"key": key}):
            destination.write_lines(
                key, map(value2json, tuid_batches(
                    source_key,
                    task_cluster_record,
                    resources,
                    generator(),
                    path="source.file"
                ))
            )

    return [key]


def count_generator():
    count = 0
    while True:
        yield count
        count += 1

