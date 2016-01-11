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
from pyLibrary.dot import Dict, wrap
from pyLibrary.env import http
from pyLibrary.env.git import get_git_revision
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator

DEBUG = False

# 06:46:57     INFO -  2015-11-24 06:46:57,398 INFO : PERFHERDER_DATA:
# 06:21:21     INFO -  PERFHERDER_DATA:
# 07:43:11     INFO -  2015-10-08 07:43:11,492 INFO : TALOSDATA:

PERFHERDER_PREFIXES = [
    b" INFO : PERFHERDER_DATA: ",
    b" INFO -  PERFHERDER_DATA: ",
    b" INFO : TALOSDATA: ",
    b" INFO -  TALOSDATA: "  # NOT SEEN IN WILD
]

EXPECTING_RESULTS = {
    "INFO - ##### Running run-tests step.": True,
    "INFO - #### Running talos suites": True,
    "========= Finished 'c:/mozilla-build/python27/python -u ...' failed (results:": False,
    "========= Finished 'c:/mozilla-build/python27/python -u ...' warnings (results:": False,
    "========= Finished '/tools/buildbot/bin/python scripts/scripts/talos_script.py ...' failed (results:": False,
    "========= Finished '/tools/buildbot/bin/python scripts/scripts/talos_script.py ...' warnings (results:": False,
    "========= Finished '/tools/buildbot/bin/python scripts/scripts/talos_script.py ...' interrupted (results:": False
}



def process(source_key, source, dest_bucket, resources, please_stop=None):
    """
    SIMPLE CONVERT pulse_block INTO PERF HERDER, IF ANY
    """
    etl_head_gen = EtlHeadGenerator(source_key)
    stats = Dict()
    counter = 0

    output = set()
    for i, pulse_line in enumerate(source.read_lines()):
        pulse_record = scrub_pulse_record(source_key, i, pulse_line, stats)
        if not pulse_record:
            continue

        if not pulse_record.payload.talos:
            continue

        test_results_expected = False
        all_perf = []
        etl_file = wrap({
            "id": counter,
            "file": pulse_record.payload.logurl,
            "timestamp": Date.now().unix,
            "revision": get_git_revision(),
            "source": pulse_record.etl,
            "type": "join"
        })
        with Timer("Read {{url}}", {"url": pulse_record.payload.logurl}, debug=DEBUG) as timer:
            try:
                response = http.get(pulse_record.payload.logurl)
                if response.status_code == 404:
                    Log.alarm("PerfHerder log missing {{url}}", url=pulse_record.payload.logurl)
                    k = source_key + "." + unicode(counter)
                    try:
                        # IF IT EXISTS WE WILL ASSUME SOME PAST PROCESS TRANSFORMED THE MISSING DATA ALREADY
                        dest_bucket.get_key(k)
                        output |= {k}  # FOR DENSITY CALCULATIONS
                    except Exception:
                        _, dest_etl = etl_head_gen.next(etl_file, "PerfHerder")
                        dest_etl.error = "PerfHerder log missing"
                        output |= dest_bucket.extend([{
                            "id": etl2key(dest_etl),
                            "value": {
                                "etl": dest_etl,
                                "pulse": pulse_record.payload
                            }
                        }])

                    continue
                all_log_lines = response.all_lines

                for log_line in all_log_lines:
                    if please_stop:
                        Log.error("Shutdown detected. Stopping early")

                    # SOME LINES GIVE US A HINT IF THERE ARE GOING TO BE TEST RESULTS
                    for pattern, result_expected in EXPECTING_RESULTS.items():
                        if pattern in log_line:
                            test_results_expected = result_expected
                            break

                    prefix = None  # prefix WILL HAVE VALUE AFTER EXITING LOOP
                    for prefix in PERFHERDER_PREFIXES:
                        s = log_line.find(prefix)
                        if s >= 0:
                            break
                    else:
                        continue

                    log_line = strings.strip(log_line[s + len(prefix):])
                    perf = convert.json2value(convert.utf82unicode(log_line))

                    if "TALOS" in prefix:
                        for t in perf:
                            _, dest_etl = etl_head_gen.next(etl_file, "Talos")
                            t.etl = dest_etl
                            t.pulse = pulse_record.payload
                        all_perf.extend(perf)
                    else: # PERFHERDER
                        for t in perf.suites:
                            _, dest_etl = etl_head_gen.next(etl_file, "PerfHerder")
                            t.framework = perf.framework
                            t.etl = dest_etl
                            t.pulse = pulse_record.payload
                        all_perf.extend(perf.suites)
            except Exception, e:
                Log.error("Problem processing {{url}}", {
                    "url": pulse_record.payload.logurl
                }, e)
            finally:
                counter += 1
                etl_head_gen.next_id = 0

        etl_file.duration = timer.duration

        if all_perf:
            if not test_results_expected:
                Log.warning("No tests run, but records found while processing {{key}}: {{url}}", key=source_key, url=pulse_record.payload.logurl)

            Log.note("Found {{num}} PerfHerder records while processing {{key}}", key=source_key, num=len(all_perf))
            output |= dest_bucket.extend([{"id": etl2key(t.etl), "value": t} for t in all_perf])
        else:
            if test_results_expected:
                Log.warning("PerfHerder records expected while processing {{key}}, but not found {{url}}", key=source_key, url=pulse_record.payload.logurl)

            _, dest_etl = etl_head_gen.next(etl_file, "PerfHerder")
            output |= dest_bucket.extend([{
                "id": etl2key(dest_etl),
                "value": {
                    "etl": dest_etl,
                    "pulse": pulse_record.payload
                }
            }])

    return output
