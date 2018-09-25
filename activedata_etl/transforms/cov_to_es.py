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

from mo_logs.strings import expand_template

from activedata_etl.imports.file_mapper import FileMapper
from activedata_etl.imports.task import minimize_task
from activedata_etl.transforms import EtlHeadGenerator, TRY_AGAIN_LATER
from activedata_etl.transforms.grcov_to_es import process_grcov_artifact
from activedata_etl.transforms.jsdcov_to_es import process_jsdcov_artifact
from activedata_etl.transforms.jsvm_to_es import process_jsvm_artifact
from activedata_etl.transforms.per_test_to_es import process_per_test_artifact
from mo_json import json2value
from mo_logs import Log, Except

DEBUG = True


def process(source_key, source, destination, resources, please_stop=None):
    """
    This transform will turn a pulse message containing info about a jsdcov artifact on taskcluster
    into a list of records of method coverages. Each record represents a method in a source file, given a test.

    :param source_key: The key of the file containing the pulse messages in the source pulse message bucket
    :param source: The source pulse messages, in a batch of (usually) 100
    :param destination: The destination for the transformed data
    :param resources: not used
    :param please_stop: The stop signal to stop the current thread
    :return: The list of keys of files in the destination bucket
    """
    keys = []
    coverage_artifact_exists = False

    for msg_line_index, msg_line in enumerate(list(source.read_lines())):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        try:
            task_cluster_record = json2value(msg_line)
        except Exception as e:
            if "JSON string is only whitespace" in e:
                continue
            else:
                Log.error("unexpected JSON decoding problem", cause=e)

        parent_etl = task_cluster_record.etl
        artifacts = task_cluster_record.task.artifacts
        minimize_task(task_cluster_record)

        etl_header_gen = EtlHeadGenerator(source_key)

        if any(  # if we will be processing coverage, then prepare the resources
            a in artifact.name
            for artifact in artifacts
            for a in ("jsdcov_artifacts.zip", "grcov", "jsvm", "per-test-coverage-reports.zip")
        ):
            if not resources.file_mapper:
                resources.file_mapper = FileMapper(task_cluster_record)

        for artifact in artifacts:
            try:
                if "jsdcov_artifacts.zip" in artifact.name:
                    pass
                    coverage_artifact_exists = True
                    _, artifact_etl = etl_header_gen.next(source_etl=parent_etl, url=artifact.url)
                    if DEBUG:
                        Log.note("Processing jsdcov artifact: {{url}}", url=artifact.url)

                    keys.extend(process_jsdcov_artifact(
                        source_key,
                        resources,
                        destination,
                        task_cluster_record,
                        artifact,
                        artifact_etl,
                        please_stop
                    ))
                elif "grcov" in artifact.name:
                    pass
                    if not task_cluster_record.repo.push.date:
                        Log.warning("expecting a repo.push.date for all tasks source_key={{key}}", key=source_key)
                        continue

                    coverage_artifact_exists = True
                    _, artifact_etl = etl_header_gen.next(source_etl=parent_etl, url=artifact.url)
                    if DEBUG:
                        Log.note("Processing grcov artifact: {{url}}", url=artifact.url)

                    keys.extend(process_grcov_artifact(
                        source_key,
                        resources,
                        destination,
                        artifact,
                        task_cluster_record,
                        artifact_etl,
                        please_stop
                    ))
                elif "jsvm" in artifact.name:
                    if not task_cluster_record.repo.push.date:
                        Log.warning("expecting a repo.push.date for all tasks source_key={{key}}", key=source_key)
                        continue

                    coverage_artifact_exists = True
                    _, artifact_etl = etl_header_gen.next(source_etl=parent_etl, url=artifact.url)
                    if DEBUG:
                        Log.note("Processing jsvm artifact: {{url}} for key {{key}}", key=source_key, url=artifact.url)

                    keys.extend(process_jsvm_artifact(
                        source_key,
                        resources,
                        destination,
                        artifact,
                        task_cluster_record,
                        artifact_etl,
                        please_stop
                    ))
                elif "per-test-coverage-reports.zip" in artifact.name:
                    try:
                        Log.warning("start per-test for {{url}}", url=artifact.url)
                        coverage_artifact_exists = True
                        _, artifact_etl = etl_header_gen.next(source_etl=parent_etl, url=artifact.url)
                        if DEBUG:
                            Log.note("Processing per-test artifact: {{url}}", url=artifact.url)

                        keys.extend(process_per_test_artifact(
                            source_key,
                            resources,
                            destination,
                            task_cluster_record,
                            artifact,
                            artifact_etl,
                            please_stop
                        ))
                    finally:
                        Log.warning("done per-test for {{url}}", url=artifact.url)

            except Exception as e:
                e = Except.wrap(e)
                reason = "Problem processing coverage: {{url}} for key {{key}}"
                params = {"url": artifact.url, "key": source_key}
                Log.warning(reason, params=params, cause=e)
                expanded_reason = expand_template(reason, params)
                raise Log.error(TRY_AGAIN_LATER, reason=expanded_reason, cause=e)

    if DEBUG and coverage_artifact_exists:
        Log.note("Done processing coverage artifacts")
    if not keys:
        return None
    else:
        return keys

