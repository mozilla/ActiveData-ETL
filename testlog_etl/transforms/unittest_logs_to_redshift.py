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
from pyLibrary.maths import Math
from pyLibrary.dot import Dict, wrap, nvl, set_default, literal_field
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key
from testlog_etl.transforms import git_revision
from testlog_etl.transforms.pulse_block_to_es import transform_buildbot


DEBUG = True


def process_unittest(source_key, source, destination, please_stop=None):
    lines = source.read_lines()

    etl_header = convert.json2value(lines[0])

    # FIX ETL IDS
    e = etl_header
    while e:
        if isinstance(e.id, basestring):
            e.id = int(e.id.split(":")[0])
        e = e.source

    bb_summary = transform_buildbot(convert.json2value(lines[1]))

    timer = Timer("Process log {{file}} for {{key}}", {
        "file": etl_header.name,
        "key": source_key
    })
    try:
        with timer:
            summary = process_unittest_log(source_key, etl_header.name, lines[2:])
    except Exception, e:
        Log.error("Problem processing {{key}}", {"key": source_key}, e)
        raise e

    bb_summary.etl = {
        "id": 0,
        "name": "unittest",
        "timestamp": Date.now().unix,
        "source": etl_header,
        "type": "join",
        "revision": git_revision,
        "duration": timer.duration.seconds
    }
    bb_summary.run.stats = summary.stats
    bb_summary.run.stats.duration = summary.stats.end_time - summary.stats.start_time

    if DEBUG:
        age = Date.now() - Date(bb_summary.run.stats.start_time * 1000)
        if age > Duration.DAY:
            Log.alert("Test is {{days|round(decimal=1)}} days old", {"days": age / Duration.DAY})
        Log.note("Done\n{{data|indent}}", {"data": bb_summary.run.stats})

    new_keys = []
    new_data = []
    for i, t in enumerate(summary.tests):
        etl = bb_summary.etl.copy()
        etl.id = i

        key = etl2key(etl)
        new_keys.append(key)

        new_data.append({
            "id": key,
            "value": set_default(
                {
                    "result": t,
                    "etl": etl
                },
                bb_summary
            )
        })
    destination.extend(new_data)
    return new_keys

