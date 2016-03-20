# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

import datetime
from copy import copy
from math import sqrt

import pyLibrary
from pyLibrary import convert
from pyLibrary.collections import MIN, MAX
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import literal_field, Dict, coalesce, unwrap, set_default, listwrap, unwraplist, wrap
from pyLibrary.dot.lists import DictList
from pyLibrary.env.git import get_git_revision
from pyLibrary.maths import Math
from pyLibrary.maths.stats import ZeroMoment2Stats, ZeroMoment
from pyLibrary.queries import qb
from pyLibrary.thread.threads import Lock
from pyLibrary.times.dates import Date
from testlog_etl.transforms.pulse_block_to_es import transform_buildbot

DEBUG = True
ARRAY_TOO_BIG = 1000
NOW = datetime.datetime.utcnow()
TOO_OLD = NOW - datetime.timedelta(days=30)
PUSHLOG_TOO_OLD = NOW - datetime.timedelta(days=7)
KNOWN_PERFHERDER_OPTIONS = ["pgo", "e10s"]
KNOWN_PERFHERDER_PROPERTIES = {"_id", "etl", "framework", "lowerIsBetter", "name", "pulse", "results", "talos_counters", "test_build", "test_machine", "testrun", "subtests", "summary", "value"}
KNOWN_PERFHERDER_TESTS = [
    "a11yr",
    "cart",
    "chromez",
    "damp",
    "dromaeo_css",
    "dromaeo_dom",
    "dromaeojs",
    "g1",
    "g2",
    "g3",
    "glterrain",
    "kraken",
    "media_tests",
    "other_nol64",
    "other_l64",
    "other",
    "sessionrestore_no_auto_restore",
    "sessionrestore",
    "svgr",
    "tabpaint",
    "tart",
    "tcanvasmark",
    "tcheck2",
    "tp4m_nochrome",
    "tp4m",
    "tp5n",
    "tp5o_scroll",
    "tp5o",
    "tpaint",
    "tps",
    "tresize",
    "trobocheck2",
    "ts_paint",
    "tscrollx",
    "tsvgr_opacity",
    "tsvgx",
    "v8_7",
    "xperf"
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
        perfherder_record=None
        try:
            perfherder_record = convert.json2value(line)
            if not perfherder_record:
                continue
            etl_source = perfherder_record.etl

            perf_records = transform(source_key, perfherder_record, resources)
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
            Log.warning("Problem with pulse payload {{pulse|json}}", pulse=perfherder_record, cause=e)
    destination.extend(records)
    return [source_key]

# CONVERT THE TESTS (WHICH ARE IN A dict) TO MANY RECORDS WITH ONE result EACH
def transform(uid, perfherder, resources):
    try:
        buildbot = transform_buildbot(perfherder.pulse, resources, uid)

        suite_name = coalesce(perfherder.testrun.suite, perfherder.name, buildbot.run.suite)

        for option in KNOWN_PERFHERDER_OPTIONS:
            if suite_name.find("-" + option) >= 0:  # REMOVE e10s REFERENCES FROM THE NAMES
                if option not in listwrap(buildbot.run.type) + listwrap(buildbot.build.type):
                    buildbot.run.type = unwraplist(listwrap(buildbot.run.type) + [option])
                    Log.warning(
                        "While processing {{uid}}, found {{option|quote}} in {{name|quote}} but not in run.type (run.type={{buildbot.run.type}}, build.type={{buildbot.build.type}})",
                        uid=uid,
                        buildbot=buildbot,
                        name=suite_name,
                        perfherder=perfherder,
                        option=option
                    )
                suite_name = suite_name.replace("-" + option, "")

        # RECOGNIZE SUITE
        for s in KNOWN_PERFHERDER_TESTS:
            if suite_name == s:
                break
            elif suite_name.startswith(s):
                Log.warning("removing suite suffix of {{suffix|quote}}", suffix=suite_name[len(s)::])
                suite_name = s
                break
            elif suite_name.startswith("remote-" + s):
                suite_name = "remote-" + s
                break
        else:
            Log.warning(
                "While processing {{uid}}, found unknown perfherder suite by name of {{name|quote}} (run.type={{buildbot.run.type}}, build.type={{buildbot.build.type}})",
                uid=uid,
                buildbot=buildbot,
                name=suite_name,
                perfherder=perfherder
            )

        # UPDATE buildbot PROPERTIES TO BETTER VALUES
        buildbot.run.timestamp = coalesce(perfherder.testrun.date, buildbot.run.timestamp)
        buildbot.run.suite = suite_name

        mainthread_transform(perfherder.results_aux)
        mainthread_transform(perfherder.results_xperf)

        new_records = DictList()

        # RECORD THE UNKNOWN PART OF THE TEST RESULTS
        if perfherder.keys() - KNOWN_PERFHERDER_PROPERTIES:
            remainder = copy(perfherder)
            for k in KNOWN_PERFHERDER_PROPERTIES:
                remainder[k] = None
            if any(remainder.values()):
                new_records.append(set_default(remainder, buildbot))

        total = DictList()

        if perfherder.subtests:
            if suite_name in ["dromaeo_css", "dromaeo_dom"]:
                #dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
                for i, subtest in enumerate(perfherder.subtests):
                    for g, sub_replicates in qb.groupby(subtest.replicates, size=5):
                        new_record = set_default(
                            {"result": {
                                "test": unicode(subtest.name) + "." + unicode(g),
                                "ordering": i,
                                "samples": sub_replicates,
                                "unit": subtest.unit,
                                "lower_is_better": subtest.lowerIsBetter
                            }},
                            buildbot
                        )
                        try:
                            s, rejects = stats(sub_replicates, subtest.name, suite_name)
                            new_record.result.stats = s
                            new_record.result.rejects = rejects
                            total.append(s)
                        except Exception, e:
                            Log.warning("can not reduce series to moments", e)
                        new_records.append(new_record)
            else:
                for i, subtest in enumerate(perfherder.subtests):
                    samples = coalesce(subtest.replicates, [subtest.value])
                    new_record = set_default(
                        {"result": {
                            "test": subtest.name,
                            "ordering": i,
                            "samples": samples,
                            "unit": subtest.unit,
                            "lower_is_better": subtest.lowerIsBetter
                        }},
                        buildbot
                    )
                    try:
                        s, rejects = stats(samples, subtest.name, suite_name)
                        new_record.result.stats = s
                        new_record.result.rejects = rejects
                        total.append(s)
                    except Exception, e:
                        Log.warning("can not reduce series to moments", e)
                    new_records.append(new_record)

        elif perfherder.results:
            #RECORD TEST RESULTS
            if suite_name in ["dromaeo_css", "dromaeo_dom"]:
                #dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
                #RECORD ALL RESULTS
                for i, (test_name, replicates) in enumerate(perfherder.results.items()):
                    for g, sub_replicates in qb.groupby(replicates, size=5):
                        new_record = set_default(
                            {"result": {
                                "test": unicode(test_name) + "." + unicode(g),
                                "ordering": i,
                                "samples": sub_replicates
                            }},
                            buildbot
                        )
                        try:
                            s, rejects = stats(sub_replicates, test_name, suite_name)
                            new_record.result.stats = s
                            new_record.result.rejects = rejects
                            total.append(s)
                        except Exception, e:
                            Log.warning("can not reduce series to moments", e)
                        new_records.append(new_record)
            else:
                for i, (test_name, replicates) in enumerate(perfherder.results.items()):
                    new_record = set_default(
                        {"result": {
                            "test": test_name,
                            "ordering": i,
                            "samples": replicates
                        }},
                        buildbot
                    )
                    try:
                        s, rejects = stats(replicates, test_name, suite_name)
                        new_record.result.stats = s
                        new_record.result.rejects = rejects
                        total.append(s)
                    except Exception, e:
                        Log.warning("can not reduce series to moments", e)
                    new_records.append(new_record)
        else:
            new_records.append(buildbot)
            Log.warning(
                "While processing {{uid}}, no `results` or `subtests` found in {{name|quote}}",
                uid=uid,
                name=suite_name
            )

        # ADD RECORD FOR GEOMETRIC MEAN SUMMARY
        buildbot.run.stats = geo_mean(total)
        Log.note(
            "Done {{uid}}, processed {{name}}, transformed {{num}} records",
            uid=uid,
            name=suite_name,
            num=len(new_records)
        )
        return new_records
    except Exception, e:
        Log.error("Transformation failure on id={{uid}}", {"uid": uid}, e)



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


def stats(given_values, test, suite):
    """
    RETURN (agg, rejects) PAIR, WHERE
    agg - LOTS OF AGGREGATES
    rejects - LIST OF VALUES NOT USED IN AGGREGATE
    """
    if given_values == None:
        return None

    rejects = unwraplist([unicode(v) for v in given_values if Math.is_nan(v)])
    clean_values = wrap([float(v) for v in given_values if not Math.is_nan(v)])

    z = ZeroMoment.new_instance(clean_values)
    s = Dict()
    for k, v in z.dict.items():
        s[k] = v
    for k, v in ZeroMoment2Stats(z).items():
        s[k] = v
    s.max = MAX(clean_values)
    s.min = MIN(clean_values)
    s.median = pyLibrary.maths.stats.median(clean_values, simple=False)
    s.last = clean_values.last()
    s.first = clean_values[0]
    if Math.is_number(s.variance) and not Math.is_nan(s.variance):
        s.std = sqrt(s.variance)

    if rejects:
        Log.warning("{{test}} in suite {{suite}} has rejects {{samples|json}}", test=test, suite=suite, samples=given_values)

    return s, rejects


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


