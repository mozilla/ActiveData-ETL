# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict
from pyLibrary.env import http
from pyLibrary.times.timer import Timer
from testlog_etl.transforms.pulse_block_to_unittest_logs import etl_key

TALOS_PREFIX = b"     INFO -  INFO : TALOSDATA: "

def process_talos(source_key, source, dest_bucket):
    """
    SIMPLE CONVERT pulse_block INTO TALOS, IF ANY
    """
    output = []
    all_talos = []
    min_dest_key = None
    min_dest_etl = None

    for i, line in enumerate(source.read().split("\n")):
        if not line.strip():
            continue
        envelope=convert.json2value(line)
        if envelope._meta:
            pass
        elif envelope.locale:
            envelope = Dict(data=envelope)
        elif envelope.source:
            continue
        elif envelope.pulse:
            # FEED THE ARRAY AS A SEQUENCE OF LINES FOR THIS METHOD TO CONTINUE PROCESSING
            def read():
                return convert.unicode2utf8("\n".join(convert.value2json(p) for p in envelope.pulse))

            temp = Dict(read=read)
            return process_talos(source_key, temp, dest_bucket)
        else:
            Log.error("Line {{index}}: Do not know how to handle line\n{{line}}", {"line": line, "index": i})

        if envelope.data.talos:
            bytes = None

            try:
                with Timer("Read {{url}}", {"url":envelope.data.logurl}):
                    response = http.get(envelope.data.logurl)
                    if response.status_code == 404:
                        Log.alarm("Talos log missing {{url}}", {"url": envelope.data.logurl})
                        continue
                    all_lines = response.all_lines

                for talos_line in all_lines:
                    s = talos_line.find(TALOS_PREFIX)
                    if s < 0:
                        continue

                    talos_line = talos_line[s + len(TALOS_PREFIX):].strip()
                    talos = convert.json2value(convert.utf82unicode(talos_line))
                    dest_key, dest_etl = etl_key(envelope, source_key, "talos")
                    if min_dest_key is None:
                        min_dest_key = dest_key
                        min_dest_etl = dest_etl

                    talos.etl = dest_etl
                    all_talos.extend(talos)

            except Exception, e:
                Log.error("Problem processing {{url}}", {
                    "url": envelope.data.logurl
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
