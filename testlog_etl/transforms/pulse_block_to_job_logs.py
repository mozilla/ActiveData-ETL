
# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
import re

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, coalesce
from pyLibrary.env import http
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY, HOUR, SECOND
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key, key2etl
from testlog_etl.imports import buildbot
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record, transform_buildbot
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator

DEBUG = False
MAX_TIMING_ERROR = SECOND  # SOME TIMESTAMPS ARE ONLY ACCURATE TO ONE SECOND

def process(source_key, source, dest_bucket, resources, please_stop=None):
    """
    SIMPLE CONVERT pulse_block INTO TALOS, IF ANY
    """
    etl_head_gen = EtlHeadGenerator(source_key)
    stats = Dict()
    counter = 0
    output = []

    for i, pulse_line in enumerate(source.read_lines()):
        pulse_record = scrub_pulse_record(source_key, i, pulse_line, stats)
        if not pulse_record:
            continue
        pulse_record.etl.source.id = key2etl(source_key).source.id

        etl_file = wrap({
            "id": counter,
            "file": pulse_record.payload.logurl,
            "timestamp": Date.now().unix,
            "source": {
                "id": 0,
                "source": pulse_record.etl,
                "type": "join"
            },
            "type": "join"
        })
        data = transform_buildbot(pulse_record.payload, resources)
        data.etl = etl_file
        with Timer("Read {{url}}", {"url": pulse_record.payload.logurl}, debug=DEBUG) as timer:
            try:
                if pulse_record.payload.logurl == None:
                    etl_file.error = "Text log missing"
                    output.append(data)
                    continue
                response = http.get(pulse_record.payload.logurl)
                if response.status_code == 404:
                    etl_file.error = "Text log unreachable"
                    output.append(data)
                    continue

                all_log_lines = response.all_lines
                data.action = process_buildbot_log(all_log_lines)

                verify_equal(data, "build.revision", "action.revision")
                verify_equal(data, "build.id", "action.buildid")
                verify_equal(data, "run.machine.name", "action.slave")

                output.append(data)
                Log.note("Found builder record for id={{id}}", id=etl2key(data.etl))
            except Exception, e:
                Log.warning("Problem processing {{url}}", url=pulse_record.payload.logurl, cause=e)
            finally:
                counter += 1
                etl_head_gen.next_id = 0

        etl_file.duration = timer.duration

    dest_bucket.extend([{"id": etl2key(d.etl), "value": d} for d in output])
    return {source_key + ".0"}



MOZLOG_STEP = re.compile(r"(\d\d:\d\d:\d\d)     INFO - ##### (Running|Skipping) (.*) step.")
MOZLOG_SUMMARY = re.compile(r"(\d\d:\d\d:\d\d)     INFO - ##### (.*) summary:")
MOZLOG_PREFIX = re.compile(r"\d\d:\d\d:\d\d     INFO - #####")


def match_mozharness_line(log_date, prev_line, curr_line, next_line):
    """
    log_date - IN %Y-%m-%d FORMAT FOR APPENDING TO THE TIME-OF-DAY STAMPS
    FOUND IN LOG LINES

    RETURN (timestamp, mode, message) PAIR IF FOUND

    EXAMPLE
    012345678901234567890123456789012345678901234567890123456789
    05:20:05     INFO - #####
    05:20:05     INFO - ##### Running download-and-extract step.
    05:20:05     INFO - #####
    """

    if len(next_line) != 25 or len(prev_line) != 25:
        return None
    if not MOZLOG_PREFIX.match(next_line) or not MOZLOG_PREFIX.match(prev_line) or not MOZLOG_PREFIX.match(curr_line):
        return None
    match = MOZLOG_STEP.match(curr_line)
    if match:
        _time, mode, message = match.group(1, 2, 3)
        mode = mode.strip().lower()
    else:
        match = MOZLOG_SUMMARY.match(curr_line)
        if not match:
            Log.warning("unexpected log line\n{{line}}", line=curr_line)
            return None
        _time, message = match.group(1, 2)
        mode = "summary"

    timestamp = Date(log_date + " " + _time, "%Y-%m-%d %H:%M:%S")
    return timestamp, mode, message


def match_builder_line(line):
    """
    RETURN (timestamp, message, done, status) QUADRUPLE

    EXAMPLES
    ========= Finished set props: build_url blobber_files (results: 0, elapsed: 0 secs) (at 2015-10-01 05:30:53.131005) =========
    ========= Started 'rm -f ...' (results: 0, elapsed: 0 secs) (at 2015-10-01 05:30:53.131322) =========
    ========= Skipped  (results: not started, elapsed: not started) =========
    """
    if not line.startswith("========= ") or not line.endswith(" ========="):
        return None

    try:
        parts = line[10:-10].strip().split("(")
        if parts[0] == "Skipped":
            # NOT THE REGULAR PATTERN
            message, status, timestamp, done = parts[0], "skipped", None, True
            return timestamp, message, done, status

        desc, stats, _time = "(".join(parts[:-2]), parts[-2], parts[-1]

    except Exception, e:
        Log.warning("Can not split log line: {{line|quote}}", line=line, cause=e)
        return None

    if desc.startswith("Started "):
        done = False
        message = desc[8:].strip()
    elif desc.startswith("Finished "):
        done = True
        message = desc[9:].strip()
    else:
        raise Log.error("Can not parse log line: {{line}}", line=line)

    result_code = int(stats.split(",")[0].split(":")[1].strip())
    status = buildbot.STATUS_CODES[result_code]

    timestamp = Date(_time[3:-1], "%Y-%m-%d %H:%M:%S.%f")

    return timestamp, message, done, status


def process_buildbot_log(all_log_lines):
    """
    Buildbot logs:

        builder: ...
        slave: ...
        starttime: ...
        results: ...
        buildid: ...
        builduid: ...
        revision: ...

        ======= <step START marker> =======
    """

    process_head = True
    data = Dict()
    data.timings = []

    start_time = None
    log_date = None
    builder_step = None
    time_zone = None

    prev_line = ""
    curr_line = ""
    next_line = ""

    for log_line in all_log_lines:

        if not log_line.strip():
            continue

        prev_line = curr_line
        curr_line = next_line
        next_line = log_line

        if process_head:
            # builder: mozilla-inbound_ubuntu32_vm_test-mochitest-e10s-browser-chrome-3
            # slave: tst-linux32-spot-149
            # starttime: 1443701997.36
            # results: success (0)
            # buildid: 20151001042128
            # builduid: 64d75a07877a458fb9f21220ae4cb5a8
            # revision: e23e76de2669b437c2f2576614c9936c713906f4
            try:
                key, value = log_line.split(": ")
                data[key] = value
                if key == "starttime":
                    data[key] = None
                    data["start_time"] = start_time = Date(float(value))
                    log_date = start_time.floor(DAY).format("%Y-%m-%d")
                if key == "results":
                    data[key] = buildbot.STATUS_CODES[value]
                continue
            except Exception, e:
                builder_says = match_builder_line(log_line)
                if not builder_says:
                    Log.warning("Log header {{log_line}} can not be processed", log_line=log_line, cause=e)
                    continue
        else:
            builder_says = match_builder_line(log_line)

        if builder_says:
            process_head = False
            timestamp, builder_step, done, status = builder_says

            if time_zone is None:
                time_zone = Math.ceiling((start_time - timestamp) / HOUR) * HOUR
            timestamp += time_zone

            if done:
                if current_step.builder.step == builder_step:
                    current_step.builder.end_time = timestamp
                    current_step.builder.status = status
                else:
                    current_step = wrap(
                        {"builder": {
                            "step": builder_step,
                            "end_time": timestamp,
                            "status": status
                        }}
                    )
                    data.timings.append(current_step)
            else:
                current_step = wrap(
                    {"builder": {
                        "step": builder_step,
                        "start_time": timestamp,
                        "status": status
                    }}
                )
                data.timings.append(current_step)
            continue

        mozharness_says = match_mozharness_line(log_date, prev_line, curr_line, next_line)
        if mozharness_says:
            timestamp, mode, harness_step = mozharness_says

            timestamp += time_zone
            if timestamp < start_time-MAX_TIMING_ERROR:
                #STARTS ON ONE DAY, AND CONTINUES IN WEE HOURS OF NEXT
                timestamp += DAY

            data.timings.append(wrap({
                "builder": {
                    "step": builder_step
                },
                "harness": {
                    "step": harness_step,
                    "mode": mode,
                    "start_time": timestamp
                }
            }))

    try:
        for e, s in qb.pairs(qb.sort(data.timings, {"value": {"coalesce": ["builder.start_time", "harness.start_time"]}, "sort": -1})):
            e.builder.duration = e.builder.end_time - s.builder.start_time
            s.builder.duration = coalesce(e.builder.start_time, s.builder.end_time) - s.builder.start_time
            s.harness.duration = coalesce(e.harness.start_time, e.builder.start_time, s.builder.end_time) - s.harness.start_time
            if s.harness.duration<0:
                Log.error("logic error")
    except Exception, e:
        Log.error("Problem with calculating durations", cause=e)
    return data


def verify_equal(data, expected, duplicate):
    """
    WILL REMOVE DUPLICATE IF NOT THE SAME
    """
    if data[expected] == data[duplicate]:
        data[duplicate] = None
    elif data[expected].startswith(data[duplicate]):
        data[duplicate] = None
    else:
        Log.warning("{{a}} != {{b}} ({{av}}!={{bv}})", a=expected, b=duplicate, av=data[expected], bv=data[duplicate])



