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

import mo_math
from activedata_etl.transforms import TRY_AGAIN_LATER
from activedata_etl.transforms.pulse_block_to_es import transform_buildbot
from mo_dots import literal_field, Data, FlatList, coalesce, unwrap, set_default, listwrap, unwraplist, wrap
from mo_logs import Log
from mo_math import MIN, MAX, Math
from mo_math.stats import ZeroMoment2Stats, ZeroMoment
from mo_threads import Lock
from mo_times.dates import Date
from pyLibrary import convert
from pyLibrary.env.git import get_git_revision
from pyLibrary.queries import jx

DEBUG = True
ARRAY_TOO_BIG = 1000
NOW = datetime.datetime.utcnow()
TOO_OLD = NOW - datetime.timedelta(days=30)
PUSHLOG_TOO_OLD = NOW - datetime.timedelta(days=7)
KNOWN_PERFHERDER_OPTIONS = ["pgo", "e10s"]
KNOWN_PERFHERDER_PROPERTIES = {"_id", "etl", "extraOptions", "framework", "is_empty", "lowerIsBetter", "name", "pulse", "results", "talos_counters", "test_build", "test_machine", "testrun", "subtests", "summary", "value"}
KNOWN_PERFHERDER_TESTS = [
    "a11yr",
    "basic_compositor_video",
    "bloom_basic_ref",
    "bloom_basic",
    "compiler warnings",
    "build times",
    "cart",
    "chromez",
    "compiler_metrics",
    "damp",
    "dromaeo_css",
    "dromaeo_dom",
    "dromaeojs",
    "GfxBench",
    "g1",
    "g2",
    "g3",
    "g4",
    "glterrain",
    "glvideo",
    "installer size",
    "jittest.jittest.overall",
    "kraken",
    "media_tests",
    "mochitest-browser-chrome",
    "other_nol64",
    "other_l64",
    "other",
    "sccache cache_write_errors",
    "sccache hit rate",
    "sccache requests_not_cacheable",
    "sessionrestore_no_auto_restore",
    "sessionrestore",
    "Strings",
    "Stylo",
    "svgr",
    "tabpaint",
    "tart",
    "TestStandardURL",
    "TreeTraversal",
    "tcanvasmark",
    "tcheck2",
    "tp4m_nochrome",
    "tp4m",
    "tp5n",
    "tp5o_scroll",
    "tp5o_webext",
    "tp5o",
    "tpaint",
    "tps",
    "tresize",
    "trobocheck2",
    "ts_paint_webext",
    "ts_paint",
    "tscrollx",
    "tsvgr_opacity",
    "tsvg_static",
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

    records = []
    i = 0
    for line in lines:
        perfherder_record = None
        try:
            perfherder_record = convert.json2value(line)
            if not perfherder_record:
                continue
            etl_source = perfherder_record.etl

            if perfherder_record.suites:
                Log.error("Should not happen, perfherder storage iterates through the suites")

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
        except Exception as e:
            if TRY_AGAIN_LATER:
                Log.error("Did not finish processing {{key}}", key=source_key, cause=e)

            Log.warning("Problem with pulse payload {{pulse|json}}", pulse=perfherder_record, cause=e)

    if not records:
        Log.warning("No perfherder records are found in {{key}}", key=source_key)

    try:
        destination.extend(records)
        return [source_key]
    except Exception as e:
        Log.error("Could not add {{num}} documents when processing key {{key}}", key=source_key, num=len(records), cause=e)


# CONVERT THE TESTS (WHICH ARE IN A dict) TO MANY RECORDS WITH ONE result EACH
def transform(source_key, perfherder, resources):
    try:
        buildbot = transform_buildbot(source_key, perfherder.pulse, resources)
        buildbot.repo.changeset.files = None
        suite_name = coalesce(perfherder.testrun.suite, perfherder.name, buildbot.run.suite)
        if not suite_name:
            if perfherder.is_empty:
                # RETURN A PLACEHOLDER
                buildbot.run.timestamp = coalesce(perfherder.testrun.date, buildbot.run.timestamp, buildbot.action.timestamp, buildbot.action.start_time)
                return [buildbot]
            else:
                Log.error("Can not process: no suite name is found")

        for option in KNOWN_PERFHERDER_OPTIONS:
            if suite_name.find("-" + option) >= 0:  # REMOVE e10s REFERENCES FROM THE NAMES
                if option not in listwrap(buildbot.run.type) + listwrap(buildbot.build.type):
                    buildbot.run.type = unwraplist(listwrap(buildbot.run.type) + [option])
                    Log.warning(
                        "While processing {{uid}}, found {{option|quote}} in {{name|quote}} but not in run.type (run.type={{buildbot.run.type}}, build.type={{buildbot.build.type}})",
                        uid=source_key,
                        buildbot=buildbot,
                        name=suite_name,
                        perfherder=perfherder,
                        option=option
                    )
                suite_name = suite_name.replace("-" + option, "")
        buildbot.run.type = list(set(buildbot.run.type + listwrap(perfherder.extraOptions)))

        # RECOGNIZE SUITE
        for s in KNOWN_PERFHERDER_TESTS:
            if suite_name == s:
                break
            elif suite_name.startswith(s):
                Log.warning("removing suite suffix of {{suffix|quote}} for {{suite}}", suffix=suite_name[len(s)::], suite=suite_name)
                suite_name = s
                break
            elif suite_name.startswith("remote-" + s):
                suite_name = "remote-" + s
                break
        else:
            if not perfherder.is_empty and perfherder.framework.name != "job_resource_usage":
                Log.warning(
                    "While processing {{uid}}, found unknown perfherder suite by name of {{name|quote}} (run.type={{buildbot.run.type}}, build.type={{buildbot.build.type}})",
                    uid=source_key,
                    buildbot=buildbot,
                    name=suite_name,
                    perfherder=perfherder
                )
                KNOWN_PERFHERDER_TESTS.append(suite_name)

        # UPDATE buildbot PROPERTIES TO BETTER VALUES
        buildbot.run.timestamp = coalesce(perfherder.testrun.date, buildbot.run.timestamp, buildbot.action.timestamp, buildbot.action.start_time)
        buildbot.run.suite = suite_name
        buildbot.run.framework = perfherder.framework

        mainthread_transform(perfherder.results_aux)
        mainthread_transform(perfherder.results_xperf)

        new_records = FlatList()

        # RECORD THE UNKNOWN PART OF THE TEST RESULTS
        if perfherder.keys() - KNOWN_PERFHERDER_PROPERTIES:
            remainder = copy(perfherder)
            for k in KNOWN_PERFHERDER_PROPERTIES:
                remainder[k] = None
            if any(remainder.values()):
                new_records.append(set_default(remainder, buildbot))

        total = FlatList()

        if perfherder.subtests:
            if suite_name in ["dromaeo_css", "dromaeo_dom"]:
                #dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
                for i, subtest in enumerate(perfherder.subtests):
                    for g, sub_replicates in jx.groupby(subtest.replicates, size=5):
                        new_record = set_default(
                            {"result": set_default(
                                stats(source_key, sub_replicates, subtest.name, suite_name),
                                {
                                    "test": unicode(subtest.name) + "." + unicode(g),
                                    "ordering": i,
                                    "unit": subtest.unit,
                                    "lower_is_better": subtest.lowerIsBetter
                                }
                            )},
                            buildbot
                        )
                        new_records.append(new_record)
                        total.append(new_record.result.stats)
            else:
                for i, subtest in enumerate(perfherder.subtests):
                    samples = coalesce(subtest.replicates, [subtest.value])
                    new_record = set_default(
                        {"result": set_default(
                            stats(source_key, samples, subtest.name, suite_name),
                            {
                                "test": subtest.name,
                                "ordering": i,
                                "unit": subtest.unit,
                                "lower_is_better": subtest.lowerIsBetter
                            }
                        )},
                        buildbot
                    )
                    new_records.append(new_record)
                    total.append(new_record.result.stats)

        elif perfherder.results:
            #RECORD TEST RESULTS
            if suite_name in ["dromaeo_css", "dromaeo_dom"]:
                #dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
                #RECORD ALL RESULTS
                for i, (test_name, replicates) in enumerate(perfherder.results.items()):
                    for g, sub_replicates in jx.groupby(replicates, size=5):
                        new_record = set_default(
                            {"result": set_default(
                                stats(source_key, sub_replicates, test_name, suite_name),
                                {
                                    "test": unicode(test_name) + "." + unicode(g),
                                    "ordering": i
                                }
                            )},
                            buildbot
                        )
                        new_records.append(new_record)
                        total.append(new_record.result.stats)
            else:
                for i, (test_name, replicates) in enumerate(perfherder.results.items()):
                    new_record = set_default(
                        {"result": set_default(
                            stats(source_key, replicates, test_name, suite_name),
                            {
                                "test": test_name,
                                "ordering": i
                            }
                        )},
                        buildbot
                    )
                    new_records.append(new_record)
                    total.append(new_record.result.stats)
        elif perfherder.value != None:  # SUITE CAN HAVE A SINGLE VALUE, AND NO SUB-TESTS
            new_record = set_default(
                {"result": set_default(
                    stats(source_key, [perfherder.value], None, suite_name),
                    {
                        "unit": perfherder.unit,
                        "lower_is_better": perfherder.lowerIsBetter
                    }
                )},
                buildbot
            )
            new_records.append(new_record)
            total.append(new_record.result.stats)
        elif perfherder.is_empty:
            buildbot.run.result.is_empty = True
            new_records.append(buildbot)
            pass
        else:
            new_records.append(buildbot)
            Log.warning(
                "While processing {{uid}}, no `results` or `subtests` found in {{name|quote}}",
                uid=source_key,
                name=suite_name
            )

        # ADD RECORD FOR GEOMETRIC MEAN SUMMARY
        buildbot.run.stats = geo_mean(total)
        Log.note(
            "Done {{uid}}, processed {{framework|upper}} :: {{name}}, transformed {{num}} records",
            uid=source_key,
            framework=buildbot.run.framework.name,
            name=suite_name,
            num=len(new_records)
        )
        return new_records
    except Exception as e:
        Log.error("Transformation failure on id={{uid}}", {"uid": source_key}, e)


def mainthread_transform(r):
    if r == None:
        return None

    output = Data()

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


def stats(source_key, given_values, test, suite):
    """
    RETURN dict WITH
    source_key - NAME OF THE SOURCE (FOR LOGGING ERRORS)
    stats - LOTS OF AGGREGATES
    samples - LIST OF VALUES USED IN AGGREGATE
    rejects - LIST OF VALUES NOT USED IN AGGREGATE
    """
    try:
        if given_values == None:
            return None

        rejects = unwraplist([unicode(v) for v in given_values if Math.is_nan(v) or not Math.is_finite(v)])
        clean_values = wrap([float(v) for v in given_values if not Math.is_nan(v) and Math.is_finite(v)])

        z = ZeroMoment.new_instance(clean_values)
        s = Data()
        for k, v in z.dict.items():
            s[k] = v
        for k, v in ZeroMoment2Stats(z).items():
            s[k] = v
        s.max = MAX(clean_values)
        s.min = MIN(clean_values)
        s.median = mo_math.stats.median(clean_values, simple=False)
        s.last = clean_values.last()
        s.first = clean_values[0]
        if Math.is_number(s.variance) and not Math.is_nan(s.variance):
            s.std = sqrt(s.variance)

        good_excuse = [
            not rejects,
            suite in ["basic_compositor_video"],
            test in ["sessionrestore_no_auto_restore"]
        ]

        if not any(good_excuse):
            Log.warning("{{test}} in suite {{suite}} in {{key}} has rejects {{samples|json}}", test=test, suite=suite, key=source_key, samples=given_values)

        return {
            "stats": s,
            "samples": clean_values,
            "rejects": rejects
        }
    except Exception as e:
        Log.warning("can not reduce series to moments", e)
        return {}


def geo_mean(values):
    """
    GIVEN AN ARRAY OF dicts, CALC THE GEO-MEAN ON EACH ATTRIBUTE
    """
    agg = Data()
    for d in values:
        for k, v in d.items():
            if v != 0:
                agg[k] = coalesce(agg[k], ZeroMoment.new_instance()) + Math.log(Math.abs(v))
    return {k: Math.exp(v.stats.mean) for k, v in agg.items()}


# TODO: deal with this
# 07:25:50     INFO - PERFHERDER_DATA: {"framework": {"name": "job_resource_usage"}, "suites": [{"subtests": [{"name": "cpu_percent", "value": 15.91289772727272}, {"name": "io_write_bytes", "value": 340640256}, {"name": "io.read_bytes", "value": 40922112}, {"name": "io_write_time", "value": 6706180}, {"name": "io_read_time", "value": 212030}], "extraOptions": ["e10s"], "name": "mochitest.mochitest-devtools-chrome.1.overall"}, {"subtests": [{"name": "time", "value": 2.5980000495910645}, {"name": "cpu_percent", "value": 10.75}], "name": "mochitest.mochitest-devtools-chrome.1.install"}, {"subtests": [{"name": "time", "value": 0.0}], "name": "mochitest.mochitest-devtools-chrome.1.stage-files"}, {"subtests": [{"name": "time", "value": 440.6840000152588}, {"name": "cpu_percent", "value": 15.960411899313495}], "name": "mochitest.mochitest-devtools-chrome.1.run-tests"}]}
