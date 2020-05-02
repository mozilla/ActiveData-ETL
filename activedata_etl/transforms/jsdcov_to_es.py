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

from zipfile import ZipFile

from activedata_etl import etl2key
from activedata_etl.imports.coverage_util import download_file, tuid_batches
from mo_dots import wrap, unwraplist, set_default
from mo_files import TempFile
from mo_json import stream, value2json
from mo_logs import Log, machine_metadata
from mo_times.dates import Date
from mo_times.timer import Timer
from mo_http.big_data import ibytes2ilines

DO_AGGR = True  # This flag will aggregate coverage information per source file.
ENABLE_METHOD_COVERAGE = False


urls_w_uncoverable_lines = set()


def process_jsdcov_artifact(source_key, resources, destination, task_cluster_record, artifact, artifact_etl, please_stop):

    def create_record(parent_etl, count, filename, covered, uncovered):
        file_details = resources.file_mapper.find(source_key, filename, artifact, task_cluster_record)

        coverable_line_count = len(covered) + len(uncovered)

        if not coverable_line_count and artifact.url not in urls_w_uncoverable_lines:
            urls_w_uncoverable_lines.add(artifact.url)
            Log.warning("jsdcov {{url}} has uncoverable lines", url=artifact.url)

        new_record = set_default(
            {
                "source": {
                    "language": "js",
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

    def process_source_file(parent_etl, count, obj):
        obj = wrap(obj)

        # get the test name. Just use the test file name at the moment
        # TODO: change this when needed
        try:
            test_name = unwraplist(obj.testUrl).split("/")[-1]
        except Exception as e:
            raise Log.error("can not get testUrl from coverage object", cause=e)

        # turn obj.covered (a list) into a set for use later
        file_covered = set(obj.covered)

        record = create_record(parent_etl, count, obj.sourceFile, set(obj.covered), set(obj.uncovered))
        record.test = {
            "name": test_name,
            "url": obj.testUrl
        }

        if ENABLE_METHOD_COVERAGE:
            # orphan lines (i.e. lines without a method), initialized to all lines
            orphan_covered = set(obj.covered)
            orphan_uncovered = set(obj.uncovered)

            # iterate through the methods of this source file
            # a variable to count the number of lines so far for this source file
            for method_name, method_lines in obj.methods.iteritems():
                all_method_lines = set(method_lines)
                method_covered = all_method_lines & file_covered
                method_uncovered = all_method_lines - method_covered
                method_percentage_covered = len(method_covered) / len(all_method_lines) if all_method_lines else None

                orphan_covered = orphan_covered - method_covered
                orphan_uncovered = orphan_uncovered - method_uncovered

                # Record method coverage info
                record.source.method = {
                    "name": method_name,
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
            for num, zip_name in enumerate(zipped.namelist()):
                for source_file_index, record in enumerate(stream.parse(zipped.open(zip_name), '.', ['.'])):
                    if please_stop:
                        Log.error("Shutdown detected. Stopping job ETL.")

                    if source_file_index == 0:
                        # this is not a jsdcov object but an object containing the version metadata
                        # TODO: this metadata should not be here
                        # TODO: this version info is not used right now. Make use of it later.
                        jsdcov_format_version = record.get("version")
                        continue

                    try:
                        for d in process_source_file(
                            artifact_etl,
                            counter,
                            record
                        ):
                            yield d
                    except Exception as e:
                        Log.warning(
                            "Error processing test {{test_url}} and source file {{source}} while processing {{key}}",
                            key=source_key,
                            test_url=record.get("testUrl"),
                            source=record.get("sourceFile"),
                            cause=e
                        )

    def aggregator():
        aggr_coverage = dict()

        with ZipFile(temp_file.abspath) as zipped:
            for num, zip_name in enumerate(zipped.namelist()):
                json_stream = ibytes2ilines(zipped.open(zip_name))
                for source_file_index, obj in enumerate(stream.parse(json_stream, '.', ['.'])):
                    if please_stop:
                        Log.error("Shutdown detected. Stopping job ETL.")

                    if source_file_index == 0:
                        # this is not a jsdcov object but an object containing the version metadata
                        # TODO: this metadata should not be here
                        # TODO: this version info is not used right now. Make use of it later.
                        jsdcov_format_version = obj.get("version")
                        continue

                    obj = wrap(obj)
                    # Collecting coverage information
                    if obj.sourceFile in aggr_coverage:
                        covered, total_lines = aggr_coverage[obj.sourceFile]
                        covered.update(obj.covered)
                        total_lines.update(obj.uncovered)  # WELL, NOT REALLY TOTAL LINES, MAY BE MISSING SOME COVERED LINES
                    else:
                        aggr_coverage[obj.sourceFile] = (set(obj.covered), set(obj.uncovered))

            # Generate coverage information per source file
            for source_file, (covered, total_lines) in aggr_coverage.items():
                uncovered = total_lines - covered
                record = create_record(artifact_etl, counter, source_file, covered, uncovered)
                yield record

    counter = count_generator().next

    with TempFile() as temp_file:
        with Timer("Downloading {{url}}", param={"url": artifact.url}):
            download_file(artifact.url, temp_file.abspath)

        key = etl2key(artifact_etl)
        with Timer("Processing JSDCov for key {{key}}", param={"key": key}):
            destination.write_lines(
                key,
                map(value2json, tuid_batches(
                    source_key,
                    task_cluster_record,
                    resources,
                    aggregator() if DO_AGGR else generator(),
                    path="source.file"
                ))
            )
        return [key]


def count_generator():
    count = 0
    while True:
        yield count
        count += 1

