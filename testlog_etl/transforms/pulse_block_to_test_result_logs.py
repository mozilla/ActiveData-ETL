# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from pyLibrary import aws

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap
from pyLibrary.times.timer import Timer
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record, transform_buildbot
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator, verify_blobber_file
from testlog_etl.transforms.unittest_logs_to_sink import process_unittest


DEBUG = False
DEBUG_SHOW_LINE = True
DEBUG_SHOW_NO_LOG = False
PARSE_TRY = True


def process(source_key, source, destination, please_stop=None):
    """
    READ pulse_block AND THE REFERENCED STRUCTURED LOG FILES
    TRANSFORM STRUCTURED LOG TO INDIVIDUAL TESTS
    """
    output = []
    stats = Dict()
    meta = aws.get_instance_metadata()
    etl_header_gen = EtlHeadGenerator(source_key)
    fast_forward = False

    existing_keys = destination.keys(prefix=source_key)
    for e in existing_keys:
        destination.delete_key(e)

    for i, line in enumerate(source.read_lines()):
        if fast_forward:
            continue
        if please_stop:
            Log.error("Stopping early")

        pulse_record = scrub_pulse_record(source_key, i, line, stats)
        if not pulse_record:
            continue

        if DEBUG or DEBUG_SHOW_LINE:
            Log.note("Source {{key}}, line {{line}}, buildid = {{buildid}}",
                key= source_key,
                line= i,
                buildid= pulse_record.data.builddate)

        file_num = 0
        for name, url in pulse_record.data.blobber_files.items():
            # USE THIS TO JUMP TO SPECIFIC FILE
            # if url != "http://mozilla-releng-blobs.s3.amazonaws.com/blobs/try/sha512/0e0b1ad188f165b1cdc28d9dbf527b96def62c736da393d86ff94a1f65366ba8cf306d3827a059100d0e6d13486b719dc2fd7c6c2dd40ab261b860b2beb37aa5":
            #     continue
            if fast_forward:
                continue
            try:
                if url == None:
                    if DEBUG:
                        Log.note("Line {{line}}: found structured log with NULL url",  line= i)
                    continue

                log_content, num_lines = verify_blobber_file(i, name, url)
                if not log_content:
                    continue

                with Timer(
                    "ETLed line {{line}}, {{name}} with {{num_lines}} lines",
                    {
                        "line": i,
                        "name": name,
                        "num_lines": num_lines
                    },
                    debug=DEBUG
                ):
                    buildbot_summary = transform_buildbot(pulse_record.data, filename=name)
                    if not PARSE_TRY and buildbot_summary.build.branch == "try":
                        continue
                    dest_key, dest_etl = etl_header_gen.next(pulse_record.data.etl, name)
                    dest_etl.instance_type = meta.instance_type
                    new_keys = process_unittest(dest_key, dest_etl, buildbot_summary, log_content, destination, please_stop=please_stop)

                    file_num += 1
                    output.append(dest_key)

                    if source.bucket.settings.fast_forward:
                        fast_forward=True

                    if DEBUG_SHOW_LINE:
                        Log.note("ETLed line {{key}}: {{url}}",
                            key= dest_key,
                            url= url)
            except Exception, e:
                Log.error("Problem processing {{name}} = {{url}}",  name= name, url=url, cause=e)

        if not file_num and DEBUG_SHOW_NO_LOG:
            Log.note("No structured log {{json}}",  json= pulse_record.data)

    if stats.num_missing_envelope:
        Log.alarm("{{num}} lines have pulse message stripped of envelope",  num= stats.num_missing_envelope)

    return output
