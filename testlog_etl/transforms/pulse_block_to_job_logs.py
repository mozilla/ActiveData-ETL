
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

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, unwraplist, Null, DictList
from pyLibrary.env import http
from pyLibrary.env.git import get_git_revision
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY, HOUR, SECOND
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key, key2etl
from testlog_etl.imports import buildbot
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record, transform_buildbot
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator

_ = convert
DEBUG = True
MAX_TIMING_ERROR = SECOND  # SOME TIMESTAMPS ARE ONLY ACCURATE TO ONE SECOND


def process(source_key, source, dest_bucket, resources, please_stop=None):
    etl_head_gen = EtlHeadGenerator(source_key)
    stats = Dict()
    counter = 0
    output = []

    for i, pulse_line in enumerate(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        pulse_record = scrub_pulse_record(source_key, i, pulse_line, stats)
        if not pulse_record:
            continue
        pulse_record.etl.source.id = key2etl(source_key).source.id

        etl = wrap({
            "id": counter,
            "file": pulse_record.payload.logurl,
            "timestamp": Date.now().unix,
            "revision": get_git_revision(),
            "source": {
                "id": 0,
                "source": pulse_record.etl,
                "type": "join"
            },
            "type": "join"
        })

        if pulse_record.payload.what == "This is a heartbeat":  # RECORD THE HEARTBEAT, OTHERWISE SOMEONE WILL ASK WHERE THE MISSING RECORDS ARE
            data = Dict(etl=etl)
            data.etl.error = "Pulse Heartbeat"
            output.append(data)
            counter += 1
            continue

        data = transform_buildbot(pulse_record.payload, resources)
        data.etl = etl
        with Timer("Read {{url}}", {"url": pulse_record.payload.logurl}, debug=DEBUG) as timer:
            try:
                if pulse_record.payload.logurl == None:
                    data.etl.error = "No logurl"
                    output.append(data)
                    continue
                response = http.get(
                    url=pulse_record.payload.logurl,
                    retry={"times": 3, "sleep": 10}
                )
                if response.status_code == 404:
                    data.etl.error = "Text log unreachable"
                    output.append(data)
                    continue

                all_log_lines = response._all_lines(encoding='latin1')
                data.action = process_buildbot_log(all_log_lines, pulse_record.payload.logurl)

                verify_equal(data, "build.revision", "action.revision")
                verify_equal(data, "build.id", "action.buildid")
                verify_equal(data, "run.machine.name", "action.slave")

                output.append(data)
                Log.note("Found builder record for id={{id}}", id=etl2key(data.etl))
            except Exception, e:
                Log.warning("Problem processing {{url}}", url=pulse_record.payload.logurl, cause=e)
                data.etl.error = "Text log unreachable"
                output.append(data)
            finally:
                counter += 1
                etl_head_gen.next_id = 0

        data.etl.duration = timer.duration

    dest_bucket.extend([{"id": etl2key(d.etl), "value": d} for d in output])
    return {source_key + ".0"}



MOZLOG_STEP = re.compile(r"(\d\d:\d\d:\d\d)     INFO - ##### (Running|Skipping) (.*) step.")
MOZLOG_SUMMARY = re.compile(r"(\d\d:\d\d:\d\d)     INFO - ##### (.*) summary:")
MOZLOG_PREFIX = re.compile(r"\d\d:\d\d:\d\d     INFO - #####")
BUILDER_ELAPSE = re.compile(r"elapsedTime=(\d+\.\d*)")  # EXAMPLE: elapsedTime=2.545

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
    if DEBUG:
        Log.note("{{line}}", line=curr_line)
    return timestamp, mode, message



last_elapse_time = None
last_elapse_time_age = 0  # KEEP TRACK OF HOW MANY LINES AGO WE SAW elapsedTime

def match_builder_line(line):
    """
    RETURN (timestamp, elapsed, message, done, status) QUADRUPLE

    THERE IS A LINE, A LITTLE BEFORE "======= Finish..." WHICH HAS elapsedTime
    EXAMPLE
    elapsedTime=2.545


    EXAMPLES
    ========= Started '/tools/buildbot/bin/python scripts/scripts/android_emulator_unittest.py ...' failed (results: 5, elapsed: 1 hrs, 12 mins, 59 secs) (at 2015-10-04 10:46:12.401377) =========
    ========= Started 'c:/mozilla-build/python27/python -u ...' warnings (results: 1, elapsed: 19 mins, 0 secs) (at 2015-10-04 07:52:22.752839) =========
    ========= Started '/tools/buildbot/bin/python scripts/scripts/b2g_emulator_unittest.py ...' interrupted (results: 4, elapsed: 22 mins, 59 secs) (at 2015-10-05 00:51:02.915315) =========
    ========= Started 'rm -f ...' (results: 0, elapsed: 0 secs) (at 2015-10-01 05:30:53.131322) =========
    c set props: build_url blobber_files (results: 0, elapsed: 0 secs) (at 2015-10-01 05:30:53.131005) =========
    ========= Skipped  (results: not started, elapsed: not started) =========
    """
    global last_elapse_time
    global last_elapse_time_age

    elapse = BUILDER_ELAPSE.match(line)
    if elapse:
        last_elapse_time = float(elapse.group(1))
        last_elapse_time_age = 0
        return
    else:
        last_elapse_time_age += 1

    if last_elapse_time_age > 3:
        # TOO LONG AGO, EXPECTING THIS NEAR THE Finish LINE
        last_elapse_time = None

    if not line.startswith("========= ") or not line.endswith(" ========="):
        return None

    try:
        parts = map(unicode.strip, line[10:-10].split("("))
        if parts[0] == "Skipped":
            # NOT THE REGULAR PATTERN
            message, status, parts, timestamp, done = "", "skipped", None, Null, True
            if DEBUG:
                Log.note("{{line}}", line=line)
            return timestamp, last_elapse_time, message, parts, done, status

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
        Log.warning("Can not parse log line: {{line}}", line=line)
        return None

    result_code = int(stats.split(",")[0].split(":")[1].strip())
    status = buildbot.STATUS_CODES[result_code]

    if message.endswith(" failed") and status in ["retry", "failure"]:
        #SOME message END WITH "failed" ON RETRY
        message = message[:-7].strip()
    elif message.endswith(" interrupted") and status in ["exception", "retry"]:
        message = message[:-12].strip()
    elif message.endswith(" " + status):
        #SOME message END WITH THE STATUS STRING
        message = message[:-(len(status) + 1)].strip()

    if message.startswith("set props: "):
        parts = unwraplist(map(unicode.strip, message[11:].split(" ")))
        message = "set props"
    else:
        parts = None

    timestamp = Date(_time[3:-1], "%Y-%m-%d %H:%M:%S.%f")

    if DEBUG:
        Log.note("{{line}}", line=line)
    return timestamp, last_elapse_time, message, parts, done, status


def process_buildbot_log(all_log_lines, from_url):
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
    end_time = None
    log_date = None
    builder_step_name = None
    builder_time_zone = None
    harness_time_zone = None

    prev_line = ""
    curr_line = ""
    next_line = ""

    for log_ascii in all_log_lines:

        if not log_ascii.strip():
            continue

        try:
            log_line = log_ascii.encode('latin1').decode('utf8')
        except Exception, e:
            if not DEBUG:
                Log.warning("Bad log line ignored while processing {{url}}\n{{line}}", url=from_url, line=log_ascii, cause=e)
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
                    data["start_time"] = start_time = end_time = Date(float(value))
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
            timestamp, elapsed, builder_step_name, parts, done, status = builder_says

            if builder_time_zone is None:
                builder_time_zone = Math.ceiling((start_time - timestamp - MAX_TIMING_ERROR) / HOUR) * HOUR
                if DEBUG:
                    Log.note("Builder time zone is {{zone}}", zone=builder_time_zone/HOUR)
            timestamp += builder_time_zone
            end_time = Math.max(end_time, timestamp)

            if done:
                if builder_step.step == builder_step_name:
                    builder_step.end_time = timestamp
                    builder_step.status = status
                    builder_step.elapsedTime = elapsed
                else:
                    builder_step = wrap({
                        "step": builder_step_name,
                        "parts": parts,
                        "start_time": builder_step.end_time,
                        "end_time": timestamp,
                        "elapsedTime": elapsed,
                        "status": status
                    })
                    data.timings.append({"builder": builder_step})
            else:
                builder_step = wrap({
                    "step": builder_step_name,
                    "parts": parts,
                    "start_time": timestamp,
                    "status": status
                })
                data.timings.append({"builder": builder_step})
            continue

        mozharness_says = match_mozharness_line(log_date, prev_line, curr_line, next_line)
        if mozharness_says:
            timestamp, mode, harness_step = mozharness_says
            if harness_time_zone is None:
                harness_time_zone = Math.ceiling((start_time - timestamp - MAX_TIMING_ERROR) / HOUR) * HOUR
                if DEBUG:
                    Log.note("Harness time zone is {{zone}}", zone=harness_time_zone / HOUR)
            timestamp += harness_time_zone
            if timestamp < start_time - MAX_TIMING_ERROR:
                #STARTS ON ONE DAY, AND CONTINUES IN WEE HOURS OF NEXT
                timestamp += DAY
            end_time = Math.max(end_time, timestamp)

            builder_step.children += [{
                "step": harness_step,
                "mode": mode,
                "start_time": timestamp
            }]

    try:
        fix_times(data.timings.builder, start_time, end_time)
        new_build_times = DictList()
        # GO IN REVERSE SO WE CAN INSERT INTO THE LIST
        for b in data.timings:
            new_build_times.append(b)
            b = b.builder
            if not b.children:
                continue

            fix_times(b.children, b.start_time, b.end_time)
            # INJECT CHILDREN INTO THIS LIST
            new_build_times.extend([
                {
                    "builder": {"step": b.step},
                    "harness": c
                }
                for c in b.children
            ])
            b.children = None

        for i, t in enumerate(new_build_times):
            t.order = i
        data.timings = new_build_times

    except Exception, e:
        Log.error("Problem with calculating durations", cause=e)
    return data


def fix_times(times, start_time, end_time):
    if start_time == None or end_time == None:
        Log.error("Require a time range")
    if not times:
        return

    time = start_time
    for i, t in enumerate(times):
        if t.start_time == None:
            # FIND BEST EVIDENCE OF WHEN THIS STARTED
            t.start_time = Math.min(Math.MIN(t.children.start_time), Math.MIN(t.children.end_time), time)
        time = Math.max(t.start_time, t.end_time, time)

    # EVERY TIME NOW HAS A start_time
    time = end_time
    for t in qb.reverse(times):
        if t.end_time == None:
            # FIND BEST EVIDENCE OF WHEN THIS ENDED (LOTS OF CANCELLED JOBS)
            t.end_time = Math.max(Math.MAX(t.children.start_time), Math.MAX(t.children.end_time), time)
        t.duration = Math.max(time, t.end_time) - t.start_time
        if t.duration==None or t.duration < 0:
            Log.error("logic error")
        time = t.start_time


def verify_equal(data, expected, duplicate):
    """
    WILL REMOVE duplicate IF THE SAME
    """
    if data[expected] == data[duplicate]:
        data[duplicate] = None
    elif data[expected].startswith(data[duplicate]):
        data[duplicate] = None
    else:
        Log.warning("{{a}} != {{b}} ({{av}}!={{bv}})", a=expected, b=duplicate, av=data[expected], bv=data[duplicate])


if __name__ == "__main__":
    response = http.get("http://ftp.mozilla.org/pub/mozilla.org/firefox/try-builds/pvanderbeken@mozilla.com-f2ccccf07663/try-linux64/try_ubuntu64_hw_test-g2-bm103-tests1-linux-build799.txt.gz")
    # for i, l in enumerate(response._all_lines(encoding="latin1")):
    #     try:
    #         l.decode('latin1').encode('utf8')
    #     except Exception:
    #         Log.alert("bad line {{num}}", num=i)
    #
    #     Log.note("{{line}}", line=l)

    data = process_buildbot_log(response.all_lines, "<unknown>")
    Log.note("{{data}}", data=data)
