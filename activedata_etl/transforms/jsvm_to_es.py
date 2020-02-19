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

from activedata_etl import etl2key
from activedata_etl.imports.coverage_util import download_file, tuid_batches
from activedata_etl.imports.parse_lcov import parse_lcov_coverage
from mo_dots import set_default
from mo_files import TempFile
from mo_future import text
from mo_json import value2json
from mo_logs import Log, machine_metadata
from mo_times import Timer, Date
from mo_http.big_data import ibytes2ilines

IGNORE_ZERO_COVERAGE = False
IGNORE_METHOD_COVERAGE = True
DEBUG = True


def process_jsvm_artifact(source_key, resources, destination, artifact, task_cluster_record, artifact_etl, please_stop):
    """
    Processes a jsvm artifact (lcov format)
    """
    if DEBUG:
        Log.note("Processing jsvm artifact {{artifact}}", artifact=artifact.url)

    file_id = etl2key(artifact_etl)
    template_record = set_default(
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
    keys = [etl_key]

    def line_gen(zipped_file):
        count = 0
        with ZipFile(zipped_file.abspath) as zipped:
            for num, zip_name in enumerate(zipped.namelist()):
                def renamed_files():
                    for source in parse_lcov_coverage(source_key, artifact.url, ibytes2ilines(zipped.open(zip_name))):
                        if please_stop:
                            return
                        if IGNORE_ZERO_COVERAGE and source.file.total_covered == 0:
                            continue
                        if IGNORE_METHOD_COVERAGE and source.file.total_covered == None:
                            continue

                        file_info = resources.file_mapper.find(source_key, source.file.name, artifact, task_cluster_record)
                        source.file = set_default(
                            file_info,
                            source.file
                        )
                        yield source

                for source in tuid_batches(
                    source_key,
                    task_cluster_record,
                    resources,
                    renamed_files(),
                    "file"
                ):
                    template_record.source = source
                    template_record.etl.id = count
                    template_record._id = file_id + "." + text(count)
                    count += 1
                    if DEBUG and (count % 10000 == 0):
                        Log.note("Processed {{num}} coverage records\n{{example}}", num=count, example=value2json(template_record))
                    yield value2json(template_record)

    with TempFile() as tmpfile:
        try:
            with Timer("download {{url}}", param={"url": artifact.url}):
                download_file(artifact.url, tmpfile.abspath)
            with Timer("Processing jsvm for key {{key}}", param={"key": etl_key}):
                destination.write_lines(file_id, line_gen(tmpfile))
        except Exception as e:
            Log.warning("problem processing jsvm artifact for key {{key}}", key=source_key, cause=e)
        return keys
