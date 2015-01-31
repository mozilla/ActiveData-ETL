# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from pympler import tracker
from time import sleep

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, Dict
from pyLibrary.env import http
from pyLibrary.thread.threads import Thread
from pyLibrary.times.timer import Timer


DEBUG = True
DEBUG_SHOW_NO_LOG = False


next_key = {}  # TRACK THE NEXT KEY FOR EACH SOURCE KEY

tr = tracker.SummaryTracker()

def process_pulse_block(source_key, source, dest_bucket):
    """
    SIMPLE CONVERT pulse_block INTO S3 LOGFILES
    PREPEND WITH ETL HEADER AND PULSE ENVELOPE
    """
    output = []
    num_missing_envelope = 0

    for i, line in enumerate(source.read().split("\n")):
        try:
            if not line.strip():
                continue
            envelope = convert.json2value(line)
            if envelope._meta:
                pass
            elif envelope.locale:
                num_missing_envelope += 1
                envelope = Dict(data=envelope)
            elif envelope.source:
                continue
            elif envelope.pulse:
                if DEBUG:
                    Log.note("Line {{index}}: found pulse array", {"index": i})
                # FEED THE ARRAY AS A SEQUENCE OF LINES FOR THIS METHOD TO CONTINUE PROCESSING
                def read():
                    return convert.unicode2utf8("\n".join(convert.value2json(p) for p in envelope.pulse))

                temp = Dict(read=read)

                return process_pulse_block(source_key, temp, dest_bucket)
            else:
                Log.error("Line {{index}}: Do not know how to handle line for key {{key}}\n{{line}}", {
                    "line": line,
                    "index": i,
                    "key":source_key
                })
        except Exception, e:
            Log.error("Line {{index}}: Problem with  line for key {{key}}\n{{line}}", {
                "line": line,
                "index": i,
                "key":source_key
            })


        file_num = 0
        for name, url in envelope.data.blobber_files.items():
            try:
                if url == None:
                    if DEBUG:
                        Log.note("Line {{index}}: found structured log with NULL url", {"index": i})
                    continue

                log_content = read_blobber_file(i, name, url)
                if not log_content:
                    continue

                dest_key, dest_etl = etl_key(envelope, source_key, name)
                if DEBUG:
                    Log.note("Line {{index}}: found structured log {{name}} for {{key}}", {
                        "index": i,
                        "name": name,
                        "key": dest_key
                    })

                dest_bucket.write(
                    dest_key,
                    convert.unicode2utf8(convert.value2json(dest_etl)) + b"\n" +  # ETL HEADER
                    convert.unicode2utf8(line) + b"\n" +  # PULSE MESSAGE
                    log_content
                )
                file_num += 1
                output.append(dest_key)
            except Exception, e:
                Log.error("Problem processing {{name}} = {{url}}", {"name": name, "url": url}, e)
            finally:
                tr.print_diff()

        if not file_num and DEBUG_SHOW_NO_LOG:
            Log.note("No structured log {{json}}", {"json": envelope.data})

    if num_missing_envelope:
        Log.alarm("{{num}} lines have pulse message stripped of envelope", {"num": num_missing_envelope})

    tr.print_diff()
    Thread.sleep(20)
    return output


def read_blobber_file(line_number, name, url):
    """
    :param line_number:  for debugging
    :param name:  for debugging
    :param url:  for debugging
    :return:  RETURNS BYTES **NOT** UNICODE
    """
    with Timer("Read {{url}}", {"url": url}, debug=DEBUG):
        response = http.get(url)
        log = response.content

    try:
        log = convert.utf82unicode(log)
    except Exception, e:
        if DEBUG:
            Log.note("Line {{index}}: {{name}} = {{url}} is NOT structured log", {
                "index": line_number,
                "name": name,
                "url": url
            })
        return None

    # DETECT IF THIS IS A STRUCTURED LOG
    total = 0  # ENSURE WE HAVE A SIDE EFFECT
    count = 0
    bad = 0
    for blobber_line in log.split("\n"):
        if not blobber_line.strip():
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
                break

    if bad > 4 and DEBUG:
        Log.note("Line {{index}}: {{name}} is NOT structured log", {
            "index": line_number,
            "name": name
        })
        return None

    return log


def etl_key(envelope, source_key, name):
    num = next_key.get(source_key, 0)
    next_key[source_key] = num + 1
    dest_key = source_key + "." + unicode(num)

    if envelope.data.etl:
        dest_etl = wrap({
            "id": num,
            "name": name,
            "source": envelope.data.etl,
            "type": "join"
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
            "type": "join"
        })
    return dest_key, dest_etl
