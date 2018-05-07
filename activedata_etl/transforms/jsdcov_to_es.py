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
from activedata_etl.imports.coverage_util import TUID_BLOCK_SIZE, download_file
from jx_python import jx
from mo_dots import wrap, unwraplist, set_default
from mo_files import TempDirectory
from mo_json import stream, value2json
from mo_logs import Log, machine_metadata
from mo_times.dates import Date
from mo_times.timer import Timer
from pyLibrary.env.big_data import ibytes2ilines

# This flag will aggregate coverage information per source file.
DO_AGGR = True


urls_w_uncoverable_lines = set()


def process_jsdcov_artifact(source_key, resources, destination, task_cluster_record, artifact, artifact_etl, please_stop):

    def create_record(parent_etl, count, filename, covered, uncovered):
        file_details = resources.file_mapper.find(source_key, filename, artifact, task_cluster_record)

        coverable_lines = len(covered) + len(uncovered)

        if not coverable_lines and artifact.url not in urls_w_uncoverable_lines:
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
                            "percentage_covered": len(covered) / coverable_lines if coverable_lines else None
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

            key = etl2key(record.etl)
            yield {"id": key, "value": record}

        # a record for all the lines that are not in any method
        # every file gets one because we can use it as canonical representative
        # Record method coverage info
        record.source.method = {
            "covered": sorted(orphan_covered),
            "uncovered": sorted(orphan_uncovered),
            "total_covered": len(orphan_covered),
            "total_uncovered": len(orphan_uncovered),
            "percentage_covered": len(orphan_covered) / max(1, (len(orphan_covered) + len(orphan_uncovered))),
        }

        # Timestamp this record
        record.etl.timestamp = Date.now()

        key = etl2key(record.etl)
        yield {"id": key, "value": record}

    def generator():
        with ZipFile(jsdcov_file) as zipped:
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

                    try:
                        for d in process_source_file(
                            artifact_etl,
                            counter,
                            obj
                        ):
                            yield d
                    except Exception as e:
                        Log.warning(
                            "Error processing test {{test_url}} and source file {{source}} while processing {{key}}",
                            key=source_key,
                            test_url=obj.get("testUrl"),
                            source=obj.get("sourceFile"),
                            cause=e
                        )

    def aggregator():
        aggr_coverage = dict()

        with ZipFile(jsdcov_file) as zipped:
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
            yield {"id": key, "value": record}

    counter = count_generator().next
    key = etl2key(artifact_etl)

    def _batch(iterator):
        """
        MARKUP THE COVERAGE RECORDS WITH TUIDS

        :param iterator: ITERATOR OF {"id": id, "value":value} objects
        :return: ITERATOR
        """
        for g, records in jx.groupby(iterator, size=TUID_BLOCK_SIZE):
            resources.tuid_mapper.annotate_sources(task_cluster_record.repo.changeset.id, [s for s in records.value])
            for r in records:
                yield value2json(r)

    with TempDirectory() as tmpdir:
        jsdcov_file = (tmpdir / "jsdcov.zip").abspath
        with Timer("Downloading {{url}}", param={"url": artifact.url}):
            download_file(artifact.url, jsdcov_file)
        with Timer("Processing JSDCov for key {{key}}", param={"key": key}):
            if DO_AGGR:
                destination.write_lines(key, _batch(aggregator()))
            else:
                destination.write_lines(key, _batch(generator()))
        keys = [key]
        return keys


def count_generator():
    count = 0
    while True:
        yield count
        count += 1

