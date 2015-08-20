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
from math import sqrt
import datetime

import pyLibrary
from pyLibrary.collections import MIN, MAX
from pyLibrary.debugs.profiles import Profiler
from pyLibrary.env.git import get_git_revision
from pyLibrary.maths import Math
from pyLibrary.maths.stats import Stats, ZeroMoment2Stats, ZeroMoment
from pyLibrary.dot import literal_field, Dict, coalesce
from pyLibrary.dot.lists import DictList
from pyLibrary.parsers import convert
from pyLibrary.thread.threads import Lock
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from testlog_etl import etl2key

from testlog_etl.imports.repos.revisions import Revision
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record, transform_buildbot
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator


DEBUG = False
ARRAY_TOO_BIG = 1000
NOW = datetime.datetime.utcnow()
TOO_OLD = NOW - datetime.timedelta(days=30)
PUSHLOG_TOO_OLD = NOW - datetime.timedelta(days=7)

repo = None
locker = Lock()
unknown_branches = set()


def process(source_key, source, destination, resources, please_stop=None):
    global repo
    if not repo:
        repo = resources.hg

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
    i = 0
    for line in lines[start:]:
        talos_record=None
        try:
            talos_record = convert.json2value(line)
            if not talos_record:
                continue

            records = transform(source_key, talos_record)
            for p in records:
                p.etl = {
                    "id": i,
                    "source": talos_record.etl,
                    "type": "join",
                    "revision": get_git_revision(),
                    "timestamp": Date.now()
                }
                key = etl2key(p.etl)
                keys.append(key)
                records.append({"id": key, "value": p})
                i += 1
        except Exception, e:
            Log.warning("Problem with pulse payload {{pulse|json}}", pulse=talos_record.payload, cause=e)
    destination.extend(records)
    return keys





# CONVERT THE TESTS (WHICH ARE IN A dict) TO MANY RECORDS WITH ONE result EACH
def transform(uid, talos_test_result):
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


        branch = r.build.branch
        if branch.lower().endswith("-non-pgo"):
            branch = branch[0:-8]
            r.build.branch = branch
            r.build.pgo = False
        else:
            r.build.pgo = True

        if r.machine.osversion.endswith(".e"):
            r.machine.osversion = r.machine.osversion[:-2]
            r.machine.e10s = True


        #ADD PUSH LOG INFO
        try:
            with Profiler("get from pushlog"):
                revision = Revision(**{"branch": {"name": branch}, "changeset": {"id": r.build.revision}})
                with locker:
                    revision = repo.get_revision(revision)

                with locker:
                    push = repo.get_push(revision)

                r.build.push_date = push.date
        except Exception, e:
            Log.warning("{{build.branch}} @ {{build.revision}} (perf_id=={{treeherder.perf_id}}) has no pushlog", r, e)
            # TRY AGAIN LATER
            return []

        new_records = []

        # RECORD THE UNKNOWN PART OF THE TEST RESULTS
        remainder = r.copy()
        remainder.results = None
        if not r.results or len(remainder.keys()) > 4:
            new_records.append(remainder)

        #RECORD TEST RESULTS
        total = DictList()
        if r.run.suite in ["dromaeo_css", "dromaeo_dom"]:
            #dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
            #RECORD ALL RESULTS
            for i, (test_name, replicates) in enumerate(r.results.items()):
                for g, sub_results in qb.groupby(replicates, size=5):
                    new_record = Dict(
                        machine=r.machine,
                        treeherder=r.treeherder,
                        run=r.run,
                        build=r.build,
                        result={
                            "test_name": unicode(test_name) + "." + unicode(g),
                            "ordering": i,
                            "samples": sub_results
                        }
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
                new_record = Dict(
                    machine=r.machine,
                    treeherder=r.treeherder,
                    run=r.run,
                    build=r.build,
                    result={
                        "test_name": test_name,
                        "ordering": i,
                        "samples": replicates
                    }
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

            new_record = Dict(
                machine=r.machine,
                treeherder=r.treeherder,
                run=r.run,
                build=r.build,
                result={
                    "test_name": "SUMMARY",
                    "ordering": -1,
                    "stats": geo_mean(total)
                }
            )
            new_records.append(new_record)

            # ADD RECORD FOR GRAPH SERVER SUMMARYh
            new_record = Dict(
                machine=r.machine,
                treeherder=r.treeherder,
                run=r.run,
                build=r.build,
                result={
                    "test_name": "summary_old",
                    "ordering": -1,
                    "stats": Stats(samples=qb.sort(total.mean)[:len(total)-1:])
                }
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


