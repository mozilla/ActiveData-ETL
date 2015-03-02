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
from pyLibrary.debugs.profiles import Profiler
from pyLibrary.env.git import get_git_revision
from pyLibrary.dot import Dict
from pyLibrary.maths import Math
from pyLibrary.times.dates import Date
from testlog_etl import etl2key, key2etl

DEBUG = True

# GET THE GIT REVISION NUMBER
git_revision = get_git_revision()


def process_pulse_block_to_es(source_key, source, destination, please_stop=None):
    lines = source.read_lines()

    etl_header = convert.json2value(lines[0])
    if etl_header.locale:
        # EARLY VERSION ETL DID NOT ADD AN ETL HEADER
        start = 0
        etl_header = key2etl(source_key)
    else:
        start = 1

    keys = []
    records=[]
    for i, line in enumerate(lines[start:]):
        line = strings.strip(line)
        if not line:
            continue
        envelope = convert.json2value(line)
        if envelope._meta:
            pass
        elif envelope.locale:
            envelope = Dict(data=envelope)

        with Profiler("transform_buildbot"):
            record = transform_buildbot(envelope.data)
            record.etl = {
                "id": i,
                "source": etl_header,
                "type": "join",
                "revision": git_revision,
                "_meta": envelope._meta,
            }
        key = etl2key(record.etl)
        keys.append(key)
        records.append({"id": key, "value": record})
    destination.extend(records)
    return keys


def transform_buildbot(payload):
    output = Dict()
    output.run.files = payload.blobber_files
    output.build.date = payload.builddate
    output.build.name = payload.buildername
    output.build.id = payload.buildid
    output.build.type = payload.buildtype
    output.build.url = payload.buildurl
    output.run.insertion_time = payload.insertion_time
    output.run.job_number = payload.job_number
    output.run.key = payload.key
    output.build.locale = payload.locale
    output.run.logurl = payload.logurl
    output.machine.os = payload.os
    output.machine.platform = payload.platform
    output.build.product = payload.product
    output.build.release = payload.release
    output.build.revision = payload.revision
    output.machine.name = payload.slave
    output.run.status = payload.status
    output.run.talos = payload.talos
    output.run.suite = payload.test
    output.run.timestamp = payload.timestamp
    output.build.branch = payload.tree

    # JUST IN CASE THERE ARE MORE PROPERTIES
    output.other = payload
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

    output.run.timestamp = Date(output.run.timestamp).unix

    output.run.files = [{"name": name, "url":url} for name, url in output.run.files.items()]

    return output
