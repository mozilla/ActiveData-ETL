# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from pyLibrary import convert, strings
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, Dict
from pyLibrary.env import http
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record


DEBUG = False
DEBUG_SHOW_LINE = True
DEBUG_SHOW_NO_LOG = False
STRUCTURED_LOG_ENDINGS = ["structured_logs.log", "_structured_full.log", '_raw.log']

next_key = {}  # TRACK THE NEXT KEY FOR EACH SOURCE KEY


def process_pulse_block(source_key, source, destination, please_stop=None):
    """
    SIMPLE CONVERT pulse_block INTO S3 LOGFILES
    PREPEND WITH ETL HEADER AND PULSE ENVELOPE
    """
    output = []
    stats=Dict()
    next_key[source_key]=0  #RESET COUNTER

    for i, line in enumerate(source.read_lines()):
        if please_stop:
            Log.error("Stopping early")

        pulse_record = scrub_pulse_record(source_key, i, line, stats)
        if not pulse_record:
            continue

        if DEBUG or DEBUG_SHOW_LINE:
            Log.note("Source {{key}}, line {{line}}, buildid = {{buildid|quote}}", {"key": source_key, "line":i, "buildid": pulse_record.data.builddate})

        file_num = 0
        for name, url in pulse_record.data.blobber_files.items():
            try:
                if url == None:
                    if DEBUG:
                        Log.note("Line {{line}}: found structured log with NULL url", {"line": i})
                    continue

                log_content, num_lines = verify_blobber_file(i, name, url)
                if not log_content:
                    continue

                with Timer(
                    "Copied {{line}}, {{name}} with {{num_lines}} lines",
                    {
                        "line": i,
                        "name": name,
                        "num_lines": num_lines
                    },
                    debug=DEBUG
                ):
                    dest_key, dest_etl = make_etl_header(pulse_record, source_key, name)

                    destination.write_lines(
                        dest_key,
                        convert.value2json(dest_etl),  # ETL HEADER
                        line,  # PULSE MESSAGE
                        log_content
                    )
                    file_num += 1
                    output.append(dest_key)

                    if DEBUG_SHOW_LINE:
                        Log.note("Copied {{key}}: {{url}}", {
                            "key": dest_key,
                            "url": url
                        })
            except Exception, e:
                Log.error("Problem processing {{name}} = {{url}}", {"name": name, "url": url}, e)

        if not file_num and DEBUG_SHOW_NO_LOG:
            Log.note("No structured log {{json}}", {"json": pulse_record.data})

    if stats.num_missing_envelope:
        Log.alarm("{{num}} lines have pulse message stripped of envelope", {"num": stats.num_missing_envelope})

    return output


def verify_blobber_file(line_number, name, url):
    """
    :param line_number:  for debugging
    :param name:  for debugging
    :param url:  TO BE READ
    :return:  RETURNS BYTES **NOT** UNICODE
    """
    if name in ["emulator-5554.log", "qemu.log"] or any(map(name.endswith, [".png", ".html"])):
        return None, 0

    with Timer("Read {{name}}: {{url}}", {"name": name, "url": url}, debug=DEBUG):
        response = http.get(url)
        try:
            logs = response.all_lines
        except Exception, e:
            if name.endswith("_raw.log"):
                Log.error("Line {{line}}: {{name}} = {{url}} is NOT structured log", {
                    "line": line_number,
                    "name": name,
                    "url": url
                }, e)

            if DEBUG:
                Log.note("Line {{line}}: {{name}} = {{url}} is NOT structured log", {
                    "line": line_number,
                    "name": name,
                    "url": url
                })
            return None, 0

    if any(name.endswith(e) for e in STRUCTURED_LOG_ENDINGS):
        # FAST TRACK THE FILES WE SUSPECT TO BE STRUCTURED LOGS ALREADY
        return logs, "unknown"

    # DETECT IF THIS IS A STRUCTURED LOG
    with Timer("Structured log detection {{name}}:", {"name": name}, debug=DEBUG):
        try:
            total = 0  # ENSURE WE HAVE A SIDE EFFECT
            count = 0
            bad = 0
            for blobber_line in logs:
                blobber_line = strings.strip(blobber_line)
                if not blobber_line:
                    continue

                try:
                    total += len(convert.json2value(blobber_line))
                    count += 1
                except Exception, e:
                    if DEBUG:
                        Log.note("Not JSON: {{line}}", {
                            "name": name,
                            "line": blobber_line
                        })
                    bad += 1
                    if bad > 4:
                        Log.error("Too many bad lines")

            if count == 0:
                # THERE SHOULD BE SOME JSON TO BE A STRUCTURED LOG
                Log.error("No JSON lines found")

        except Exception, e:
            if name.endswith("_raw.log") and "No JSON lines found" not in e:
                Log.error("Line {{line}}: {{name}} is NOT structured log", {
                    "line": line_number,
                    "name": name
                }, e)
            if DEBUG:
                Log.note("Line {{line}}: {{name}} is NOT structured log", {
                    "line": line_number,
                    "name": name
                })
            return None, 0

    return logs, count


def make_etl_header(envelope, source_key, name):
    num = next_key[source_key]
    next_key[source_key] = num + 1
    dest_key = source_key + "." + unicode(num)

    if envelope.data.etl:
        dest_etl = wrap({
            "id": num,
            "name": name,
            "source": envelope.data.etl,
            "type": "join",
            "timestamp": Date.now().unix
        })
    else:
        if source_key.endswith(".json"):
            Log.error("Not expected")

        dest_etl = wrap({
            "id": num,
            "name": name,
            "source": {
                "id": source_key
            },
            "type": "join",
            "timestamp": Date.now().unix
        })
    return dest_key, dest_etl
