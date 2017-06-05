# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from activedata_etl.imports.task import minimize_task
from activedata_etl.transforms import verify_blobber_file, EtlHeadGenerator
from activedata_etl.transforms.unittest_logs_to_sink import process_unittest
from mo_dots import listwrap
from pyLibrary import convert
from mo_logs import Log, machine_metadata
from mo_times.dates import Date

DEBUG = False
DEBUG_SHOW_LINE = True
DEBUG_SHOW_NO_LOG = False
PARSE_TRY = True

SINGLE_URL = None


def process(source_key, source, destination, resources, please_stop=None):
    """
    READ pulse_block AND THE REFERENCED STRUCTURED LOG FILES
    TRANSFORM STRUCTURED LOG TO INDIVIDUAL TESTS
    """
    output = []
    etl_header_gen = EtlHeadGenerator(source_key)

    existing_keys = destination.keys(prefix=source_key)
    for e in existing_keys:
        destination.delete_key(e)

    file_num = 0
    lines = list(source.read_lines())

    for i, line in enumerate(lines):
        if please_stop:
            Log.error("Shutdown detected. Stopping early")

        task = convert.json2value(line)
        etl = task.etl
        artifacts = task.task.artifacts
        minimize_task(task)

        # REVIEW THE ARTIFACTS, LOOK FOR STRUCTURED LOGS
        for j, a in enumerate(listwrap(artifacts)):
            if Date(a.expires) < Date.now():
                Log.note("Expired url: expires={{date}} url={{url}}", date=Date(a.expires), url=a.url)
                continue  # ARTIFACT IS GONE
            lines, num_bytes = verify_blobber_file(j, a.name, a.url)
            if lines:
                dest_key, dest_etl = etl_header_gen.next(etl, a.name)
                dest_etl.machine = machine_metadata
                dest_etl.url = a.url
                process_unittest(dest_key, dest_etl, task, lines, destination, please_stop=please_stop)
                file_num += 1
                output.append(dest_key)

    return output


