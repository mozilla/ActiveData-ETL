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
from pyLibrary.dot import Dict, wrap, coalesce
from pyLibrary.env import http, elasticsearch
from pyLibrary.env.git import get_git_revision
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer
from activedata_etl import etl2key
from activedata_etl.transforms.pulse_block_to_es import scrub_pulse_record
from activedata_etl.transforms import EtlHeadGenerator

DEBUG = False

# 06:46:57     INFO -  2015-11-24 06:46:57,398 INFO : PERFHERDER_DATA:
# 06:21:21     INFO -  PERFHERDER_DATA:
# 07:43:11     INFO -  2015-10-08 07:43:11,492 INFO : TALOSDATA:

PERFHERDER_PREFIXES = [
    b" INFO : PERFHERDER_DATA: ",
    b" INFO -  PERFHERDER_DATA: ",
    b" INFO - PERFHERDER_DATA: ",
    b" INFO : TALOSDATA: ",
    b" INFO -  TALOSDATA: ",  # NOT SEEN IN WILD
    b" INFO - TALOSDATA: "  # NOT SEEN IN WILD
]


def process(source_key, source, dest_bucket, resources, please_stop=None):
    """
    CONVERT pulse_block INTO PERFHERDER, IF ANY
    """
    etl_head_gen = EtlHeadGenerator(source_key)
    stats = Dict()
    counter = 0

    output = set()
    for i, pulse_line in enumerate(source.read_lines()):
        pulse_record = scrub_pulse_record(source_key, i, pulse_line, stats)
        if not pulse_record:
            continue

        etl_file = wrap({
            "id": counter,
            "timestamp": Date.now().unix,
            "revision": get_git_revision(),
            "source": pulse_record.etl,
            "type": "join"
        })

        propslist = coalesce(pulse_record.payload.build.properties, pulse_record.payload.properties)
        if isinstance(propslist, list):
            props = wrap({k: v for k, v, s in propslist})
        else:
            props = propslist
        log_url = coalesce(pulse_record.payload.logurl, pulse_record.payload.log_url, props.logurl, props.log_url)
        if not log_url:
            if convert.value2json(pulse_record).find("logurl") != -1:
                Log.warning("{{key}} line {{line}} has no logurl\n{{record|json}}", key=source_key, line=i, record=pulse_record)

            # _, dest_etl = etl_head_gen.next(etl_file, "PerfHerder")
            # output |= dest_bucket.extend([{
            #     "id": etl2key(dest_etl),
            #     "value": {
            #         "etl": dest_etl,
            #         "pulse": pulse_record.payload,
            #         "is_empty": True
            #     }
            # }])
            continue

        with Timer("Read {{url}}", {"url": log_url}, debug=DEBUG) as timer:
            try:
                response = http.get(log_url)
                if response.status_code == 404:
                    Log.alarm("PerfHerder log missing {{url}}", url=log_url)
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
                                "pulse": pulse_record.payload,
                                "is_empty": True
                            }
                        }])

                    continue
                seen, all_perf = extract_perfherder(response.all_lines, etl_file, etl_head_gen, please_stop, pulse_record)
            except Exception, e:
                Log.error("Problem processing {{url}}", {
                    "url": log_url
                }, e)
            finally:
                try:
                    response.close()
                except Exception:
                    pass
                counter += 1
                etl_head_gen.next_id = 0

        etl_file.file = log_url
        etl_file.duration = timer.duration

        if all_perf:
            Log.note("Found {{num}} PerfHerder records while processing {{key}} {{i}}: {{url}}", key=source_key, i=i, num=len(all_perf), url=log_url)
            output |= dest_bucket.extend([{"id": etl2key(t.etl), "value": t} for t in all_perf])
        else:
            _, dest_etl = etl_head_gen.next(etl_file, "PerfHerder")
            output |= dest_bucket.extend([{
                "id": etl2key(dest_etl),
                "value": {
                    "etl": dest_etl,
                    "pulse": pulse_record.payload,
                    "is_empty": True
                }
            }])

    return output


def extract_perfherder(all_log_lines, etl_file, etl_head_gen, please_stop, pulse_record):
    perfherder_exists = False
    all_perf = []

    for log_line in all_log_lines:
        if please_stop:
            Log.error("Shutdown detected. Stopping early")

        prefix = None  # prefix WILL HAVE VALUE AFTER EXITING LOOP
        for prefix in PERFHERDER_PREFIXES:
            s = log_line.find(prefix)
            if s >= 0:
                perfherder_exists = True
                break
        else:
            continue

        log_line = strings.strip(log_line[s + len(prefix):])
        perf = convert.json2value(convert.utf82unicode(log_line))

        if "TALOS" in prefix:
            for t in perf:
                _, dest_etl = etl_head_gen.next(etl_file, "talos")
                t.etl = dest_etl
                t.pulse = pulse_record.payload
            all_perf.extend(perf)
        else:  # PERFHERDER
            for t in perf.suites:
                _, dest_etl = etl_head_gen.next(etl_file, "PerfHerder")
                t.framework = perf.framework
                t.etl = dest_etl
                t.pulse = pulse_record.payload
            all_perf.extend(perf.suites)
    return perfherder_exists, all_perf
