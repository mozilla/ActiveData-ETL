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
from pyLibrary.dot import Dict, wrap, Null, coalesce, listwrap
from pyLibrary.maths import Math
from pyLibrary.times.dates import Date
from testlog_etl import etl2key
from testlog_etl.imports.repos.changesets import Changeset
from testlog_etl.imports.repos.revisions import Revision

DEBUG = True


def process(source_key, source, destination, resources, please_stop=None):
    lines = source.read_lines()

    etl_header = convert.json2value(lines[0])
    if etl_header.etl:
        start = 0
    elif etl_header.locale or etl_header._meta:
        start = 0
    else:
        start = 1

    keys = []
    records = []
    stats = Dict()
    for i, line in enumerate(lines[start:]):
        pulse_record = Null
        try:
            pulse_record = scrub_pulse_record(source_key, i, line, stats)
            if not pulse_record:
                continue

            with Profiler("transform_buildbot"):
                record = transform_buildbot(pulse_record.payload, resources=resources)
                record.etl = {
                    "id": i,
                    "source": pulse_record.etl,
                    "type": "join",
                    "revision": get_git_revision()
                }
            key = etl2key(record.etl)
            keys.append(key)
            records.append({"id": key, "value": record})
        except Exception, e:
            Log.warning("Problem with pulse payload {{pulse|json}}", pulse=pulse_record.payload, cause=e)
    destination.extend(records)
    return keys


def scrub_pulse_record(source_key, i, line, stats):
    """
    DUE TO MANY ETL FORMATS, THIS IS REQUIRED TO
    TURN RAW LINE INTO A STANDARD PULSE RECORD
    """
    try:
        line = strings.strip(line)
        if not line:
            return None
        pulse_record = convert.json2value(line)
        if pulse_record._meta:
            pulse_record.etl.source.id = pulse_record.etl.source.count  # REMOVE AFTER JULY 1 2015, JUST A FEW RECORDS HAVE THIS PROBLEM
            return pulse_record
        elif pulse_record.locale:
            stats.num_missing_envelope += 1
            pulse_record = wrap({
                "payload": pulse_record,
                "etl": pulse_record.etl
            })
            return pulse_record
        else:
            if i == 0 and pulse_record.source:
                #OLD-STYLE ETL HAD A HEADER RECORD
                return None

            Log.warning(
                "Line {{index}}: Do not know how to handle line for key {{key}}\n{{line}}",
                line=line,
                index=i,
                key=source_key
            )
            return None
    except Exception, e:
        Log.warning(
            "Line {{index}}: Problem with line for key {{key}}\n{{line}}",
            line=line,
            index=i,
            key=source_key,
            cause=e
        )



def transform_buildbot(payload, resources, filename=None):
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
    output.run.machine.os = payload.os
    output.machine.os = payload.os
    output.build.platform = payload.platform
    output.build.product = payload.product
    output.build.release = payload.release
    output.build.revision = payload.revision
    output.build.revision12 = payload.revision[0:12]
    output.run.machine.name = payload.slave
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

    try:
        output.repo = resources.hg.get_revision(Revision(branch={"name": output.build.branch}, changeset=Changeset(id=output.build.revision)))
    except Exception, e:
        if "Unknown push" in e:
            cause = listwrap(e.cause)[0]
            Log.note(cause.template, param=cause.param)
        else:
            Log.warning("Can not get revision ({{branch}}, {{revision}})", revision=output.build.revision, branch=output.build.branch, cause=e)

    return output
