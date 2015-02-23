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
from pyLibrary.dot import wrap, Dict
from pyLibrary.env import http
from pyLibrary.times.timer import Timer


DEBUG = False
DEBUG_SHOW_NO_LOG = False


next_key = {}  # TRACK THE NEXT KEY FOR EACH SOURCE KEY


def process_pulse_block(source_key, source, dest_bucket):
    """
    SIMPLE CONVERT pulse_block INTO S3 LOGFILES
    PREPEND WITH ETL HEADER AND PULSE ENVELOPE
    """
    output = []
    num_missing_envelope = 0

    for i, line in enumerate(source.read_lines()):
        try:
            line = strings.strip(line)
            if not line:
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
            Log.warning("Line {{index}}: Problem with line for key {{key}}\n{{line}}", {
                "line": line,
                "index": i,
                "key": source_key
            }, e)


        file_num = 0
        for name, url in envelope.data.blobber_files.items():
            try:
                if url == None:
                    if DEBUG:
                        Log.note("Line {{index}}: found structured log with NULL url", {"index": i})
                    continue

                log_content, num_lines = read_blobber_file(i, name, url)
                if not log_content:
                    continue

                with Timer(
                    "Copied {{name}} with {{num_lines}} lines)",
                    {
                        "index": i,
                        "name": name,
                        "num_lines": num_lines
                    },
                    debug=DEBUG
                ):
                    dest_key, dest_etl = etl_key(envelope, source_key, name)

                    dest_bucket.write_lines(
                        dest_key,
                        convert.value2json(dest_etl),  # ETL HEADER
                        line,  # PULSE MESSAGE
                        log_content
                    )
                    file_num += 1
                    output.append(dest_key)
            except Exception, e:
                Log.error("Problem processing {{name}} = {{url}}", {"name": name, "url": url}, e)

        if not file_num and DEBUG_SHOW_NO_LOG:
            Log.note("No structured log {{json}}", {"json": envelope.data})

    if num_missing_envelope:
        Log.alarm("{{num}} lines have pulse message stripped of envelope", {"num": num_missing_envelope})

    return output


def read_blobber_file(line_number, name, url):
    """
    :param line_number:  for debugging
    :param name:  for debugging
    :param url:  for debugging
    :return:  RETURNS BYTES **NOT** UNICODE
    """
    if name in ["emulator-5554.log", "qemu.log"] or any(map(name.endswith, [".png", ".html"])):
        return None, 0

    with Timer("Read {{name}}: {{url}}", {"name":name, "url": url}, debug=DEBUG):
        response = http.get(url)
        try:
            logs = response.all_lines
        except Exception, e:
            if name.endswith("_raw.log"):
                Log.error("Line {{index}}: {{name}} = {{url}} is NOT structured log", {
                    "index": line_number,
                    "name": name,
                    "url": url
                }, e)

            if DEBUG:
                Log.note("Line {{index}}: {{name}} = {{url}} is NOT structured log", {
                    "index": line_number,
                    "name": name,
                    "url": url
                })
            return None, 0

    # DETECT IF THIS IS A STRUCTURED LOG
    try:
        total = 0  # ENSURE WE HAVE A SIDE EFFECT
        count = 0
        bad = 0
        for blobber_line in logs:
            blobber_line = strings.strip(blobber_line)
            if not blobber_line:
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
                    Log.error("Too many bad lines")

        if count == 0:
            # THERE SHOULD BE SOME JSON TO BE A STRUCTURED LOG
            Log.error("No JSON lines found")

    except Exception, e:
        if name.endswith("_raw.log") and "No JSON lines found" not in e:
            Log.error("Line {{index}}: {{name}} is NOT structured log", {
                "index": line_number,
                "name": name
            }, e)
        if DEBUG:
            Log.note("Line {{index}}: {{name}} is NOT structured log", {
                "index": line_number,
                "name": name
            })
        return None, 0

    return logs, count


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
