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

from pyLibrary import convert
from pyLibrary.debugs.logs import Log, machine_metadata
from pyLibrary.dot import Dict, set_default, Null
from pyLibrary.env import http
from pyLibrary.thread.threads import Signal
from pyLibrary.times.timer import Timer
from activedata_etl.transforms.pulse_block_to_es import scrub_pulse_record, transform_buildbot
from activedata_etl.transforms import EtlHeadGenerator, verify_blobber_file
from activedata_etl.transforms.unittest_logs_to_sink import process_unittest


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
    stats = Dict()
    etl_header_gen = EtlHeadGenerator(source_key)
    fast_forward = False

    existing_keys = destination.keys(prefix=source_key)
    for e in existing_keys:
        destination.delete_key(e)

    all_lines = list(enumerate(convert.utf82unicode(source.read()).split("\n")))  # NOT EXPECTED TO BE BIG, AND GENERATOR MAY TAKE TOO LONG
    for i, line in all_lines:
        if fast_forward:
            continue
        if please_stop:
            Log.error("Shutdown detected. Stopping early")

        pulse_record = scrub_pulse_record(source_key, i, line, stats)
        if not pulse_record:
            continue

        if DEBUG or DEBUG_SHOW_LINE:
            Log.note(
                "Source {{key}}, line {{line}}, buildid = {{buildid}}",
                key=source_key,
                line=i,
                buildid=pulse_record.payload.builddate
            )

        file_num = 0
        for name, url in pulse_record.payload.blobber_files.items():
            if SINGLE_URL is not None and url != SINGLE_URL:
                continue
            if fast_forward:
                continue
            try:
                if url == None:
                    if DEBUG:
                        Log.note("Line {{line}}: found structured log with NULL url", line=i)
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
                    buildbot_summary = transform_buildbot(source_key, pulse_record.payload, resources, filename=name)
                    if not PARSE_TRY and buildbot_summary.build.branch == "try":
                        continue
                    dest_key, dest_etl = etl_header_gen.next(pulse_record.etl, name)
                    dest_etl.machine = machine_metadata
                    dest_etl.url = url
                    new_keys = process_unittest(dest_key, dest_etl, buildbot_summary, log_content, destination, please_stop=please_stop)

                    file_num += 1
                    output.append(dest_key)

                    if source.bucket.settings.fast_forward:
                        fast_forward = True

                    if DEBUG_SHOW_LINE:
                        Log.note(
                            "ETLed line {{key}}: {{url}}",
                            key=dest_key,
                            url=url
                        )
            except Exception, e:
                Log.error("Problem processing {{name}} = {{url}}", name=name, url=url, cause=e)

        if not file_num and DEBUG_SHOW_NO_LOG:
            Log.note("No structured log {{json}}", json=pulse_record.payload)

    if stats.num_missing_envelope:
        Log.alarm("{{num}} lines have pulse message stripped of envelope", num=stats.num_missing_envelope)

    return output


if __name__ == "__main__":
    response = http.get("http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win32/1444321537/mozilla-inbound_xp-ix_test-g2-e10s-bm119-tests1-windows-build710.txt.gz")

    def extend(data):
        for d in data:
            Log.note("{{data}}", data=d)

    destination = Dict(extend=extend)

    try:
        _new_keys = process_unittest("0:0.0.0", Dict(), Dict(), response.all_lines, destination, please_stop=Signal())
    finally:
        response.close()

