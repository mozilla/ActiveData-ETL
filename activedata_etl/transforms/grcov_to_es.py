# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (klahnakoski@mozilla.com)

from __future__ import division
from __future__ import unicode_literals

from zipfile import ZipFile

from activedata_etl.imports.tuid_client import TuidClient
from mo_future import text_type

from activedata_etl import etl2key
from activedata_etl.imports.file_mapper import FileMapper
from activedata_etl.imports.parse_lcov import parse_lcov_coverage
from activedata_etl.transforms import download_file
from mo_dots import set_default
from mo_files import TempFile
from mo_json import value2json
from mo_logs import Log, machine_metadata
from mo_times import Timer, Date
from pyLibrary.env.big_data import ibytes2ilines

IGNORE_ZERO_COVERAGE = False
IGNORE_METHOD_COVERAGE = True
RETRY = {"times": 3, "sleep": 5}
DEBUG = True


def process_grcov_artifact(source_key, resources, destination, grcov_artifact, task_cluster_record, artifact_etl, please_stop):
    """
    Processes a grcov artifact (lcov format)
    """
    if DEBUG:
        Log.note("Processing grcov artifact {{artifact}}", artifact=grcov_artifact.url)

    file_id = etl2key(artifact_etl)
    new_record = set_default(
        {
            "test": {
                "suite": task_cluster_record.run.suite.name,
                "chunk": task_cluster_record.run.chunk
            },
            "etl": {
                "source": artifact_etl,
                "type": "join",
                "machine": machine_metadata,
                "timestamp": Date.now()
            }
        },
        task_cluster_record
    )
    etl_key = etl2key(artifact_etl)
    keys = [etl_key]  #

    with TempFile() as tmpfile:
        with Timer("download {{url}}", param={"url": grcov_artifact.url}):
            download_file(grcov_artifact.url, tmpfile.abspath)
        with Timer("Processing grcov for key {{key}}", param={"key": etl_key}):
            def line_gen():
                count = 0
                with ZipFile(tmpfile.abspath) as zipped:
                    for num, zip_name in enumerate(zipped.namelist()):
                        if num == 1:
                            Log.error("expecting only one artifact in the grcov.zip file while processing {{key}}", key=source_key)
                        for source in parse_lcov_coverage(source_key, grcov_artifact.url, ibytes2ilines(zipped.open(zip_name))):
                            if please_stop:
                                return
                            if IGNORE_ZERO_COVERAGE and not source.file.total_covered == 0:
                                continue
                            if IGNORE_METHOD_COVERAGE and source.file.total_covered == None:
                                continue
                            file_info = resources.file_mapper.find(source_key, source.file.name, grcov_artifact, task_cluster_record)
                            resources.tuid_mapper.annotate_source(task_cluster_record.repo.changeset.id, source)
                            new_record.source = set_default(
                                {"file": file_info},
                                source
                            )
                            new_record.etl.id = count
                            new_record._id = file_id + "." + text_type(count)
                            count += 1
                            if DEBUG and (count % 10000 == 0):
                                Log.note("Processed {{num}} coverage records\n{{example}}", num=count, example=value2json(new_record))
                            yield value2json(new_record)
            destination.write_lines(file_id, line_gen())

        return keys
