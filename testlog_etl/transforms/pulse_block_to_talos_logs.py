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
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator

DEBUG = False

TALOS_PREFIX = b"     INFO -  INFO : TALOSDATA: "


def process(source_key, source, dest_bucket, please_stop=None):
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

        if not pulse_record.data.talos:
            continue

        all_talos = []
        etl_file = wrap({
            "id": counter,
            "file": pulse_record.data.logurl,
            "timestamp": Date.now().unix,
            "source": pulse_record.data.etl,
            "type": "join"
        })
        with Timer("Read {{url}}", {"url": pulse_record.data.logurl}, debug=DEBUG) as timer:
            try:
                response = http.get(pulse_record.data.logurl)
                if response.status_code == 404:
                    Log.alarm("Talos log missing {{url}}", url=pulse_record.data.logurl)
                    k = source_key + "." + unicode(counter)
                    try:
                        # IF IT EXISTS WE WILL ASSUME SOME PAST PROCESS TRANSFORMED THE MISSING DATA ALREADY
                        dest_bucket.get_key(k)
                        output |= {k}  # FOR DENSITY CALCULATIONS
                    except Exception, _:
                        _, dest_etl = etl_head_gen.next(etl_file, "talos")
                        dest_etl.error = "Talos log missing"
                        output |= dest_bucket.extend([{
                            "id": etl2key(dest_etl),
                            "value": {
                                "etl": dest_etl,
                                "pulse": pulse_record.data
                            }
                        }])

                    continue
                all_log_lines = response.all_lines

                for log_line in all_log_lines:
                    s = log_line.find(TALOS_PREFIX)
                    if s < 0:
                        continue

                    log_line = strings.strip(log_line[s + len(TALOS_PREFIX):])
                    talos = convert.json2value(convert.utf82unicode(log_line))

                    for t in talos:
                        _, dest_etl = etl_head_gen.next(etl_file, "talos")
                        t.etl = dest_etl
                        t.pulse = pulse_record.data
                    all_talos.extend(talos)
            except Exception, e:
                Log.error("Problem processing {{url}}", {
                    "url": pulse_record.data.logurl
                }, e)
            finally:
                counter += 1
                etl_head_gen.next_id = 0

        etl_file.duration = timer.seconds

        if all_talos:
            Log.note("Found {{num}} talos records", num=len(all_talos))
            output |= dest_bucket.extend([{"id": etl2key(t.etl), "value": t} for t in all_talos])
        else:
            Log.note("No talos records found in {{url}}", url=pulse_record.data.logurl)
            _, dest_etl = etl_head_gen.next(etl_file, "talos")

            output |= dest_bucket.extend([{
                "id": etl2key(dest_etl),
                "value": {
                    "etl": dest_etl,
                    "pulse": pulse_record.data
                }
            }])

    return output
