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
from pyLibrary.dot import Dict
from pyLibrary.env import http
from pyLibrary.times.timer import Timer
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record
from testlog_etl.transforms.pulse_block_to_unittest_logs import make_etl_header

DEBUG = False

TALOS_PREFIX = b"     INFO -  INFO : TALOSDATA: "

def process_talos(source_key, source, dest_bucket, please_stop=None):
    """
    SIMPLE CONVERT pulse_block INTO TALOS, IF ANY
    """
    output = []
    all_talos = []
    min_dest_key = None
    min_dest_etl = None
    stats = Dict()

    for i, line in enumerate(source.read().split("\n")):
        pulse_record = scrub_pulse_record(source_key, i, line, stats)
        if not pulse_record:
            continue

        if not pulse_record.data.talos:
            continue

        try:
            with Timer("Read {{url}}", {"url":pulse_record.data.logurl}, debug=DEBUG):
                response = http.get(pulse_record.data.logurl)
                if response.status_code == 404:
                    Log.alarm("Talos log missing {{url}}", {"url": pulse_record.data.logurl})
                    continue
                all_lines = response.all_lines

            for talos_line in all_lines:
                s = talos_line.find(TALOS_PREFIX)
                if s < 0:
                    continue

                talos_line = strings.strip(talos_line[s + len(TALOS_PREFIX):])
                talos = convert.json2value(convert.utf82unicode(talos_line))
                dest_key, dest_etl = make_etl_header(pulse_record, source_key, "talos")
                if min_dest_key is None:
                    min_dest_key = dest_key
                    min_dest_etl = dest_etl

                talos.etl = dest_etl
                all_talos.extend(talos)

        except Exception, e:
            Log.error("Problem processing {{url}}", {
                "url": pulse_record.data.logurl
            }, e)

    if all_talos:
        Log.note("found {{num}} talos records", {"num": len(all_talos)})
        dest_bucket.write(
            min_dest_key,
            convert.unicode2utf8(convert.value2json(min_dest_etl)) + b"\n" +
            convert.unicode2utf8("\n".join(convert.value2json(t) for t in all_talos))
        )
        output.append(source_key)

    return output
