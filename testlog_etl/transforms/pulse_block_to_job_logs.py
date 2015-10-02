
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
from pyLibrary.dot import Dict, wrap, literal_field, coalesce
from pyLibrary.env import http
from pyLibrary.queries import qb
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator

DEBUG = False



def process(source_key, source, dest_bucket, resources, please_stop=None):
    """
    SIMPLE CONVERT pulse_block INTO TALOS, IF ANY
    """
    etl_head_gen = EtlHeadGenerator(source_key)
    stats = Dict()
    counter = 0

    output = set()
    for i, pulse_line in enumerate(source.read_lines()):
        pulse_record = scrub_pulse_record(source_key, i, pulse_line, stats)
        if not pulse_record:
            continue

        all_builder = []
        etl_file = wrap({
            "id": counter,
            "file": pulse_record.payload.logurl,
            "timestamp": Date.now().unix,
            "source": pulse_record.etl,
            "type": "join"
        })
        with Timer("Read {{url}}", {"url": pulse_record.payload.logurl}, debug=DEBUG) as timer:
            try:
                response = http.get(pulse_record.payload.logurl)
                if response.status_code == 404:
                    Log.alarm("Text log missing {{url}}", url=pulse_record.payload.logurl)
                    k = source_key + "." + unicode(counter)
                    try:
                        # IF IT EXISTS WE WILL ASSUME SOME PAST PROCESS TRANSFORMED THE MISSING DATA ALREADY
                        dest_bucket.get_key(k)
                        output |= {k}  # FOR DENSITY CALCULATIONS
                    except Exception:
                        _, dest_etl = etl_head_gen.next(etl_file, "text log")
                        dest_etl.error = "Text log missing"
                        output |= dest_bucket.extend([{
                            "id": etl2key(dest_etl),
                            "value": {
                                "etl": dest_etl,
                                "pulse": pulse_record.payload
                            }
                        }])

                    continue
                all_log_lines = response.all_lines

                data = process_buildbot_log(all_log_lines)
                all_builder.extend(data)
            except Exception, e:
                Log.error("Problem processing {{url}}", {
                    "url": pulse_record.payload.logurl
                }, e)
            finally:
                counter += 1
                etl_head_gen.next_id = 0

        etl_file.duration = timer.duration

        if all_builder:
            Log.note("Found {{num}} builder records", num=len(all_builder))
            output |= dest_bucket.extend([{"id": etl2key(t.etl), "value": t} for t in all_builder])
        else:
            Log.note("No talos records found in {{url}}", url=pulse_record.payload.logurl)
            _, dest_etl = etl_head_gen.next(etl_file, "talos")

            output |= dest_bucket.extend([{
                "id": etl2key(dest_etl),
                "value": {
                    "etl": dest_etl,
                    "pulse": pulse_record.payload
                }
            }])

    return output



MOZLOG_STEP = re.compile(r"(\d\d:\d\d:\d\d)     INFO - ##### Running (.*) step.")
MOZLOG_PREFIX = re.compile(r"\d\d:\d\d:\d\d     INFO - #####")


def match_mozharness_line(log_date, prev_line, curr_line, next_line):
    """
    log_date - IN %Y-%m-%d FORMAT FOR APPENDING TO THE TIME-OF-DAY STAMPS
    FOUND IN LOG LINES

    RETURN (timestamp, message) PAIR IF FOUND

    EXAMPLE
    012345678901234567890123456789012345678901234567890123456789
    05:20:05     INFO - #####
    05:20:05     INFO - ##### Running download-and-extract step.
    05:20:05     INFO - #####
    """

    if len(next_line) != 25 or len(prev_line) != 25:
        return None
    if not MOZLOG_PREFIX.match(next_line) or not MOZLOG_PREFIX.match(prev_line):
        return None
    _time, message = MOZLOG_STEP.match(curr_line).group(1, 2)

    timestamp = Date(log_date + " " + _time, "%Y-%m-%d %H:%M:%S")

    return timestamp, message


def match_builder_line(line):
    """
    RETURN (timestamp, message, done) TRIPLE

    EXAMPLES
    ========= Finished set props: build_url blobber_files (results: 0, elapsed: 0 secs) (at 2015-10-01 05:30:53.131005) =========
    ========= Started 'rm -f ...' (results: 0, elapsed: 0 secs) (at 2015-10-01 05:30:53.131322) =========
    """
    if not line.startswith("=========") or not line.endswith("========="):
        return None

    desc, stats, _time = line[9:-9].strip().split("(")

    if desc.startswith("Started "):
        done = False
        message = desc[8:].strip()
    elif desc.startswith("Finished "):
        done = True
        message = desc[9:].strip()
    else:
        raise Log.error("not expected")

    timestamp = Date(_time[3:-1], "%Y-%m-%d %H:%M:%S.%f")

    return timestamp, message, done


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
    data.timings = UniqueIndex(keys=("name",))

    start_time = None
    log_date = None
    builder_step = None

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
                    data["start_time"] = start_time = Date(float(value))
                    log_date = start_time.floor(DAY).format("%Y-%m-%d")
            except Exception, e:
                Log.warning("Log header {{log_line}} can not be processed", log_line=log_line, cause=e)

            continue

        builder_says = match_builder_line(log_line)
        if builder_says:
            process_head = True

            timestamp, builder_step, done = builder_says
            if done:
                if current_step.builder_step != builder_step:
                    current_step = {"builder_step": builder_step, "builder_start_time": timestamp}
                    data.timings.add(current_step)
                else:
                    current_step.builder_end_time = timestamp
            else:
                current_step = {"builder_step": builder_step, "builder_start_time": timestamp}
                data.timings.add(current_step)
            continue

        mozharness_says = match_mozharness_line(log_date, prev_line, curr_line, next_line)
        if mozharness_says:
            timestamp, harness_step = mozharness_says
            if timestamp < start_time:
                #STARTS ON ONE DAY, AND CONTINUES IN WEE HOURS OF NEXT
                timestamp += DAY

            data.timings.add({"builder_step": builder_step, "harness_step": harness_step, "harness_time": timestamp})

    for e, s in qb.pairs(qb.sort(data.timings, {"start_time": "desc"})):
        s.builder_duration = coalesce(e.builder_start_time, s.builder_end_time) - s.builder_start_time
        s.harness_duration = coalesce(e.harness_time, e.builder_start_time, s.builder_end_time) - s.harness_time

    return data
