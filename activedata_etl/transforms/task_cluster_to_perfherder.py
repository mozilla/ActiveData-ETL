# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from future.utils import text_type

from activedata_etl import etl2key
from activedata_etl.imports.task import minimize_task
from activedata_etl.transforms import EtlHeadGenerator
from activedata_etl.transforms.pulse_block_to_es import scrub_pulse_record
from activedata_etl.transforms.pulse_block_to_perfherder_logs import PERFHERDER_PREFIXES
from mo_dots import Data, wrap, Null, unwraplist, FlatList
from mo_json import json2value, utf82unicode
from mo_logs import Log, strings, suppress_exception
from mo_times import Date
from mo_times.timer import Timer
from pyLibrary.env import http

DEBUG = False


def process(source_key, source, dest_bucket, resources, please_stop=None):
    """
    CONVERT pulse_block INTO PERFHERDER, IF ANY
    """
    stats = Data()

    output = set()
    for i, pulse_line in enumerate(list(source.read_lines())):
        etl_header_gen = EtlHeadGenerator(source_key)
        pulse_record = scrub_pulse_record(source_key, i, pulse_line, stats)
        if not pulse_record:
            continue

        etl_task = pulse_record.etl
        artifacts = pulse_record.task.artifacts
        minimize_task(pulse_record)

        all_perf = FlatList()
        with Timer("get perfherder records"):
            for artifact in artifacts:
                if artifact.name.endswith("perfherder-data.json"):
                    perf = http.get_json(artifact.url)
                    for t in perf.suites:
                        t.framework = perf.framework
                        t.task = pulse_record
                        _, t.etl = etl_header_gen.next(etl_task, url=artifact.url)
                    all_perf.extend(perf.suites)
                    Log.note(
                        "Found {{num}} {{framework|upper}} records while processing {{key}} {{i}}: {{url}}",
                        key=source_key,
                        i=i,
                        num=len(perf.suites),
                        framework=perf.framework.name,
                        url=artifact.url
                    )

            log_url = wrap([a.url for a in artifacts if a.name.endswith("/live_backing.log")])[0]

            # PULL PERFHERDER/TALOS
            if log_url:
                try:
                    response = http.get(log_url)
                    if response.status_code == 404:
                        Log.alarm("PerfHerder log missing {{url}}", url=log_url)
                        k = source_key + "." + text_type(i)
                        try:
                            # IF IT EXISTS WE WILL ASSUME SOME PAST PROCESS TRANSFORMED THE MISSING DATA ALREADY
                            dest_bucket.get_key(k)
                            output |= {k}  # FOR DENSITY CALCULATIONS
                        except Exception:
                            _, dest_etl = etl_header_gen.next(etl_task, name="PerfHerder", url=log_url, error="PerfHerder log missing")
                            output |= dest_bucket.extend([{
                                "id": etl2key(dest_etl),
                                "value": {
                                    "etl": dest_etl,
                                    "task": pulse_record,
                                    "is_empty": True
                                }
                            }])

                        continue
                    seen, more_perf = extract_perfherder(
                        response.get_all_lines(flexible=True),
                        etl_task,
                        etl_header_gen,
                        please_stop,
                        pulse_record
                    )
                    all_perf.extend(more_perf)
                except Exception as e:
                    Log.error("Problem processing {{url}}", url=log_url, cause=e)
                finally:
                    with suppress_exception:
                        response.close()

        if all_perf:
            Log.note(
                "Found {{num}} {{framework|upper}} records while processing {{key}} {{i}}: {{url}}",
                key=source_key,
                i=i,
                framework=unwraplist(list(set(all_perf.framework.name))),
                num=len(all_perf),
                url=log_url
            )
            output |= dest_bucket.extend([{"id": etl2key(t.etl), "value": t} for t in all_perf])
        else:
            Log.note("Found zero PerfHerder records while processing {{key}} {{i}}: {{url}}", key=source_key, i=i, url=log_url)
            _, dest_etl = etl_header_gen.next(etl_task, name="PerfHerder")
            output |= dest_bucket.extend([{
                "id": etl2key(dest_etl),
                "value": {
                    "etl": {"id": 0, "source": dest_etl, "timestamp": Date.now()},
                    "task": pulse_record,
                    "is_empty": True
                }
            }])

    return output


def extract_perfherder(all_log_lines, etl_job, etl_header_gen, please_stop, pulse_record):
    perfherder_exists = False
    all_perf = []
    line_number = Null
    log_line = Null

    try:
        for line_number, log_line in enumerate(all_log_lines):
            if please_stop:
                Log.error("Shutdown detected. Stopping early")

            for prefix in PERFHERDER_PREFIXES:
                s = log_line.find(prefix)
                if s >= 0:
                    perfherder_exists = True
                    break
            else:
                continue

            log_line = strings.strip(log_line[s + len(prefix):])
            perf = json2value(utf82unicode(log_line))

            if "TALOS" in prefix:
                for t in perf:
                    t.task = pulse_record
                    _, t.etl = etl_header_gen.next(etl_job, name="talos")
                all_perf.extend(perf)
            else:  # PERFHERDER
                for t in perf.suites:
                    t.framework = perf.framework
                    t.task = pulse_record
                    _, t.etl = etl_header_gen.next(etl_job, name="PerfHerder")
                all_perf.extend(perf.suites)
    except Exception as e:
        Log.error("Can not read line after #{{num}}\nPrevious line = {{line|quote}}", num=line_number, line=log_line, cause=e)
    return perfherder_exists, all_perf
