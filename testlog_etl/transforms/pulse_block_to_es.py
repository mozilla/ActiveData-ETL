# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division

from pyLibrary import convert, strings
from pyLibrary.debugs.logs import Log
from pyLibrary.debugs.profiles import Profiler
from pyLibrary.env.git import get_git_revision
from pyLibrary.dot import Dict, wrap
from pyLibrary.maths import Math
from pyLibrary.times.dates import Date
from testlog_etl import etl2key, key2etl

DEBUG = True

# GET THE GIT REVISION NUMBER
git_revision = get_git_revision()


def process(source_key, source, destination, please_stop=None):
    lines = source.read_lines()

    etl_header = convert.json2value(lines[0])
    if etl_header.locale:
        # EARLY VERSION ETL DID NOT ADD AN ETL HEADER
        start = 0
        etl_header = key2etl(unicode(source_key))
    else:
        start = 1

    keys = []
    records = []
    stats = Dict()
    for i, line in enumerate(lines[start:]):
        pulse_record = scrub_pulse_record(source_key, i, line, stats)
        if not pulse_record:
            continue

        with Profiler("transform_buildbot"):
            record = transform_buildbot(pulse_record.data)
            record.etl = {
                "id": i,
                "source": etl_header,
                "type": "join",
                "revision": git_revision,
                "_meta": pulse_record._meta,
            }
        key = etl2key(record.etl)
        keys.append(key)
        records.append({"id": key, "value": record})
    destination.extend(records)
    return keys


def scrub_pulse_record(source_key, i, line, stats):
    try:
        line = strings.strip(line)
        if not line:
            return None
        pulse_record = convert.json2value(line)
        if pulse_record._meta:
            return pulse_record
        elif pulse_record.locale:
            stats.num_missing_envelope += 1
            pulse_record = wrap({"data": pulse_record})
            return pulse_record
        elif pulse_record.source:
            return None
        elif pulse_record.pulse:
            Log.error("Does this happen?")
            # if DEBUG:
            #     Log.note("Line {{index}}: found pulse array",  index= i)
            # # FEED THE ARRAY AS A SEQUENCE OF LINES FOR THIS METHOD TO CONTINUE PROCESSING
            # def read():
            #     return convert.unicode2utf8("\n".join(convert.value2json(p) for p in pulse_record.pulse))
            #
            # temp = Dict(read=read)
            #
            # return process_pulse_block(source_key, temp, destination)
        else:
            Log.error("Line {{index}}: Do not know how to handle line for key {{key}}\n{{line}}",
                line= line,
                index= i,
                key= source_key)
    except Exception, e:
        Log.warning("Line {{index}}: Problem with line for key {{key}}\n{{line}}",
            line=line,
            index=i,
            key=source_key,
            cause=e
        )



def transform_buildbot(payload, filename=None):
    output = Dict()
    output.run.files = payload.blobber_files
    output.build.date = payload.builddate
    output.build.name = payload.buildername
    output.build.id = payload.buildid
    output.build.type = payload.buildtype
    output.build.url = payload.buildurl
    output.run.job_number = payload.job_number

    # TODO: THESE SHOULD BE ETL PROPERTIES
    output.run.insertion_time = payload.insertion_time
    output.run.key = payload.key

    output.build.locale = payload.locale
    output.run.logurl = payload.logurl
    output.machine.os = payload.os
    output.build.platform = payload.platform
    output.build.product = payload.product
    output.build.release = payload.release
    output.build.revision = payload.revision
    output.machine.name = payload.slave

    # payload.status IS THE BUILDBOT STATUS
    # https://github.com/mozilla/pulsetranslator/blob/acf495738f8bd119f64820958c65e348aa67963c/pulsetranslator/pulsetranslator.py#L295
    # https://hg.mozilla.org/build/buildbot/file/fbfb8684802b/master/buildbot/status/builder.py#l25
    output.run.status = payload.status   # TODO: REMOVE EVENTUALLY
    try:
        output.run.buildbot_status = {
            0: "success",
            1: "warnings",
            2: "failure",
            3: "skipped",
            4: "exception",
            5: "retry",
            6: "cancelled",
            None: None
        }[payload.status]
    except Exception, e:
        Log.warning("It seems the Pulse payload status {{status|quote}} has no string representative", status=payload.status)

    output.run.talos = payload.talos
    output.run.suite = payload.test
    output.run.timestamp = Date(payload.timestamp).unix
    output.build.branch = payload.tree

    # JUST IN CASE THERE ARE MORE PROPERTIES
    output.other = payload = payload.copy()
    payload.blobber_files = None
    payload.builddate = None
    payload.buildername = None
    payload.buildid = None
    payload.buildtype = None
    payload.buildurl = None
    payload.etl = None
    payload.insertion_time = None
    payload.job_number = None
    payload.key = None
    payload.locale = None
    payload.logurl = None
    payload.os = None
    payload.platform = None
    payload.product = None
    payload.release = None
    payload.revision = None
    payload.slave = None
    payload.status = None
    payload.talos = None
    payload.test = None
    payload.timestamp = None
    payload.tree = None

    path = output.run.suite.split("-")
    if Math.is_integer(path[-1]):
        output.run.chunk = int(path[-1])
        output.run.suite = "-".join(path[:-1])

    output.run.files = [
        {"name": name, "url": url}
        for name, url in output.run.files.items()
        if filename is None or name == filename
    ]

    return output
