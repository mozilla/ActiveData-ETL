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

import json
from zipfile import ZipFile

from activedata_etl import etl2key
from activedata_etl.imports.coverage_util import TUID_BLOCK_SIZE, download_file
from jx_python import jx
from mo_dots import wrap, set_default
from mo_files import TempDirectory
from mo_json import stream, value2json
from mo_logs import Log, machine_metadata
from mo_times.dates import Date
from mo_times.timer import Timer


urls_w_uncoverable_lines = set()


def process_per_test_artifact(source_key, resources, destination, task_cluster_record, artifact, artifact_etl, please_stop):

    def create_record(parent_etl, count, filename, covered, uncovered):
        file_details = resources.file_mapper.find(source_key, filename, artifact, task_cluster_record)

        coverable_lines = len(covered) + len(uncovered)

        if not coverable_lines and artifact.url not in urls_w_uncoverable_lines:
            urls_w_uncoverable_lines.add(artifact.url)
            Log.warning("per-test-coverage {{url}} has uncoverable lines", url=artifact.url)

        new_record = set_default(
            {
                "source": {
                    "file": set_default(
                        file_details,
                        {
                            "covered": covered,
                            "uncovered": uncovered,
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

    def process_source_file(parent_etl, count, suite, test, sf):
        sf = wrap(sf)

        covered = []
        uncovered = []
        coverable = []
        for l, c in enumerate(sf["coverage"]):
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

        record = create_record(parent_etl, count, sf["name"], covered, uncovered)
        record.test = {
            "name": test,
            "suite": suite,
        }

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
        with ZipFile(per_test_file) as zipped:
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
                            "Error processing test {{test}} and source file {{source}} while processing {{key}}",
                            key=source_key,
                            test=test,
                            source=sf['name'],
                            cause=e
                        )

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
        per_test_file = (tmpdir / "per_test.zip").abspath
        with Timer("Downloading {{url}}", param={"url": artifact.url}):
            download_file(artifact.url, per_test_file)
        with Timer("Processing per-test reports for key {{key}}", param={"key": key}):
            destination.write_lines(key, _batch(generator()))
        keys = [key]
        return keys


def count_generator():
    count = 0
    while True:
        yield count
        count += 1

