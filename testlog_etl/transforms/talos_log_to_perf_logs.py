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
from copy import copy
from math import sqrt
import datetime

import pyLibrary
from pyLibrary.collections import MIN, MAX
from pyLibrary.env.git import get_git_revision
from pyLibrary.maths import Math
from pyLibrary.maths.stats import ZeroMoment2Stats, ZeroMoment
from pyLibrary.dot import literal_field, Dict, coalesce, unwrap, set_default
from pyLibrary.dot.lists import DictList
from pyLibrary.parsers import convert
from pyLibrary.thread.threads import Lock
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from testlog_etl.transforms.pulse_block_to_es import transform_buildbot


DEBUG = False
ARRAY_TOO_BIG = 1000
NOW = datetime.datetime.utcnow()
TOO_OLD = NOW - datetime.timedelta(days=30)
PUSHLOG_TOO_OLD = NOW - datetime.timedelta(days=7)
KNOWN_TALOS_PROPERTIES = {"results", "run", "etl", "pulse", "summary", "test_build", "test_machine", "_id", "talos_counters"}
KNOWN_TALOS_TESTS = [
    "tp5o", "dromaeo_css", "dromaeo_dom", "tresize", "tcanvasmark", "tcheck2",
    "tsvgx", u'tp4m', u'tp5n', u'a11yr', u'ts_paint', "tpaint",
    "sessionrestore_no_auto_restore", "sessionrestore", "tps", "damp",
    "kraken", "tsvgr_opacity", "tart", "tscrollx", "cart", "v8_7", "glterrain",
    "xxx"
]

repo = None
locker = Lock()
unknown_branches = set()


def process(source_key, source, destination, resources, please_stop=None):
    global repo
    if repo is None:
        repo = unwrap(resources.hg)

    lines = source.read_lines()

    etl_header = convert.json2value(lines[0])
    if etl_header.etl:
        start = 0
    elif etl_header.locale or etl_header._meta:
        start = 0
    else:
        start = 1

    records = []
    i = 0
    for line in lines[start:]:
        talos_record=None
        try:
            talos_record = convert.json2value(line)
            if not talos_record:
                continue
            etl_source = talos_record.etl

            perf_records = transform(source_key, talos_record, resources)
            for p in perf_records:
                p["etl"] = {
                    "id": i,
                    "source": etl_source,
                    "type": "join",
                    "revision": get_git_revision(),
                    "timestamp": Date.now()
                }
                key = source_key + "." + unicode(i)
                records.append({"id": key, "value": p})
                i += 1
        except Exception, e:
            Log.warning("Problem with pulse payload {{pulse|json}}", pulse=talos_record, cause=e)
    destination.extend(records)
    return [source_key]





# CONVERT THE TESTS (WHICH ARE IN A dict) TO MANY RECORDS WITH ONE result EACH
def transform(uid, talos_test_result, resources):
    try:
        r = talos_test_result

        def mainthread_transform(r):
            if r == None:
                return None

            output = Dict()

            for i in r.mainthread_readbytes:
                output[literal_field(i[1])].name = i[1]
                output[literal_field(i[1])].readbytes = i[0]
            r.mainthread_readbytes = None

            for i in r.mainthread_writebytes:
                output[literal_field(i[1])].name = i[1]
                output[literal_field(i[1])].writebytes = i[0]
            r.mainthread_writebytes = None

            for i in r.mainthread_readcount:
                output[literal_field(i[1])].name = i[1]
                output[literal_field(i[1])].readcount = i[0]
            r.mainthread_readcount = None

            for i in r.mainthread_writecount:
                output[literal_field(i[1])].name = i[1]
                output[literal_field(i[1])].writecount = i[0]
            r.mainthread_writecount = None

            r.mainthread = output.values()

        mainthread_transform(r.results_aux)
        mainthread_transform(r.results_xperf)

        buildbot = transform_buildbot(r.pulse, resources, uid)

        # RENAME PROPERTIES
        r.run, r.testrun = r.testrun, None
        r.run.timestamp, r.run.date = r.run.date, None

        # RECOGNIZE SUITE
        for s in KNOWN_TALOS_TESTS:
            if r.run.suite.startswith(s):
                r.run.suite = s
                break
        else:
            Log.warning("Do not know talos suite by name of {{name}}", name=r.run.suite)

        Log.note("Process Talos {{name}}", name=r.run.suite)
        new_records = DictList()

        # RECORD THE UNKNOWN PART OF THE TEST RESULTS
        if r.keys() - KNOWN_TALOS_PROPERTIES:
            remainder = copy(r)
            for k in KNOWN_TALOS_PROPERTIES:
                remainder[k] = None
            new_records.append(set_default(remainder, buildbot))

        #RECORD TEST RESULTS
        total = DictList()
        if r.run.suite in ["dromaeo_css", "dromaeo_dom"]:
            #dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
            #RECORD ALL RESULTS
            for i, (test_name, replicates) in enumerate(r.results.items()):
                for g, sub_results in qb.groupby(replicates, size=5):
                    new_record = set_default(
                        {"result": {
                            "test": unicode(test_name) + "." + unicode(g),
                            "ordering": i,
                            "samples": sub_results
                        }},
                        buildbot
                    )
                    try:
                        s = stats(sub_results)
                        new_record.result.stats = s
                        total.append(s)
                    except Exception, e:
                        Log.warning("can not reduce series to moments", e)
                    new_records.append(new_record)
        else:
            for i, (test_name, replicates) in enumerate(r.results.items()):
                new_record = set_default(
                    {"result": {
                        "test": test_name,
                        "ordering": i,
                        "samples": replicates
                    }},
                    buildbot
                )
                try:
                    s = stats(replicates)
                    new_record.result.stats = s
                    total.append(s)
                except Exception, e:
                    Log.warning("can not reduce series to moments", e)
                new_records.append(new_record)

        if len(total) > 1:
            # ADD RECORD FOR GEOMETRIC MEAN SUMMARY

            new_record = set_default(
                {"result": {
                    "test": "SUMMARY",
                    "ordering": -1,
                    "stats": geo_mean(total)
                }},
                buildbot
            )
            new_records.append(new_record)

        return new_records
    except Exception, e:
        Log.error("Transformation failure on id={{uid}}", {"uid": uid}, e)


def stats(values):
    """
    RETURN LOTS OF AGGREGATES
    """
    if values == None:
        return None

    values = values.map(float, includeNone=False)

    z = ZeroMoment.new_instance(values)
    s = Dict()
    for k, v in z.dict.items():
        s[k] = v
    for k, v in ZeroMoment2Stats(z).items():
        s[k] = v
    s.max = MAX(values)
    s.min = MIN(values)
    s.median = pyLibrary.maths.stats.median(values, simple=False)
    s.last = values.last()
    s.first = values[0]
    if Math.is_number(s.variance) and not Math.is_nan(s.variance):
        s.std = sqrt(s.variance)

    return s


def geo_mean(values):
    """
    GIVEN AN ARRAY OF dicts, CALC THE GEO-MEAN ON EACH ATTRIBUTE
    """
    agg = Dict()
    for d in values:
        for k, v in d.items():
            if v != 0:
                agg[k] = coalesce(agg[k], ZeroMoment.new_instance()) + Math.log(Math.abs(v))
    return {k: Math.exp(v.stats.mean) for k, v in agg.items()}


