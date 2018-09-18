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

from mo_future import text_type
from jx_python import jx
from mo_dots import literal_field, Data, FlatList, coalesce, unwrap, set_default, listwrap, unwraplist, wrap
from mo_json import json2value
from mo_logs import Log
from mo_math import MIN, MAX, Math
from mo_threads import Lock

import mo_math
from activedata_etl.transforms import TRY_AGAIN_LATER
from activedata_etl.transforms.pulse_block_to_es import transform_buildbot
from mo_math.stats import ZeroMoment2Stats, ZeroMoment
from mo_times.dates import Date
from pyLibrary.env.git import get_git_revision

DEBUG = True
ARRAY_TOO_BIG = 1000
NOW = datetime.datetime.utcnow()
TOO_OLD = NOW - datetime.timedelta(days=30)
PUSHLOG_TOO_OLD = NOW - datetime.timedelta(days=7)


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
            perfherder_record = json2value(line)
            if not perfherder_record:
                continue
            etl_source = perfherder_record.etl

            if perfherder_record.suites:
                Log.error("Should not happen, perfherder storage iterates through the suites")

            if perfherder_record.pulse:
                metadata = transform_buildbot(source_key, perfherder_record.pulse, resources)
                perfherder_record.pulse = None
            elif perfherder_record.task or perfherder_record.is_empty:
                metadata, perfherder_record.task = perfherder_record.task, None
            else:
                Log.warning("Expecting some task/job information. key={{key}}", key=perfherder_record._id)
                continue

            if not isinstance(metadata.run.suite, text_type):
                metadata.run.suite = metadata.run.suite.fullname

            perf_records = transform(source_key, perfherder_record, metadata, resources)
            for p in perf_records:
                p["etl"] = {
                    "id": i,
                    "source": etl_source,
                    "type": "join",
                    "revision": get_git_revision(),
                    "timestamp": Date.now()
                }
                key = source_key + "." + text_type(i)
                records.append({"id": key, "value": p})
                i += 1
        except Exception as e:
            if TRY_AGAIN_LATER:
                Log.error("Did not finish processing {{key}}", key=source_key, cause=e)

            Log.warning("Problem with pulse payload {{pulse|json}}", pulse=perfherder_record, cause=e)

    if not records:
        Log.warning("No perfherder records are found in {{key}}", key=source_key)

    try:
        destination.extend(records, overwrite=True)
        return [source_key]
    except Exception as e:
        Log.error("Could not add {{num}} documents when processing key {{key}}", key=source_key, num=len(records), cause=e)


# CONVERT THE TESTS (WHICH ARE IN A dict) TO MANY RECORDS WITH ONE result EACH
def transform(source_key, perfherder, metadata, resources):
    if perfherder.is_empty:
        return [metadata]

    try:
        framework_name = perfherder.framework.name
        suite_name = coalesce(perfherder.testrun.suite, perfherder.name, metadata.run.suite)
        if not suite_name:
            if perfherder.is_empty:
                # RETURN A PLACEHOLDER
                metadata.run.timestamp = coalesce(perfherder.testrun.date, metadata.run.timestamp, metadata.action.timestamp, metadata.action.start_time)
                return [metadata]
            else:
                Log.error("Can not process: no suite name is found")

        for option in KNOWN_PERFHERDER_OPTIONS:
            if suite_name.find("-" + option) >= 0:
                if option == 'coverage':
                    pass  # coverage matches "jsdcov" and many others, do not bother sending warnings if not found
                elif option not in listwrap(metadata.run.type) + listwrap(metadata.build.type) and framework_name != 'job_resource_usage':
                    Log.warning(
                        "While processing {{uid}}, found {{option|quote}} in {{name|quote}} but not in run.type (run.type={{metadata.run.type}}, build.type={{metadata.build.type}})",
                        uid=source_key,
                        metadata=metadata,
                        name=suite_name,
                        perfherder=perfherder,
                        option=option
                    )
                    metadata.run.type = unwraplist(listwrap(metadata.run.type) + [option])
                suite_name = suite_name.replace("-" + option, "")


        # RECOGNIZE SUITE
        for s in KNOWN_PERFHERDER_TESTS:
            if suite_name == s:
                break
            elif suite_name.startswith(s) and framework_name != 'job_resource_usage':
                Log.warning(
                    "While processing {{uid}}, removing suite suffix of {{suffix|quote}} for {{suite}} in framwork {{framework}}",
                    uid=source_key,
                    suffix=suite_name[len(s)::],
                    suite=suite_name,
                    framework=framework_name
                )
                suite_name = s
                break
            elif suite_name.startswith("remote-" + s):
                suite_name = "remote-" + s
                break
        else:
            if not perfherder.is_empty and framework_name != "job_resource_usage":
                Log.warning(
                    "While processing {{uid}}, found unknown perfherder suite by name of {{name|quote}} (run.type={{metadata.run.type}}, build.type={{metadata.build.type}})",
                    uid=source_key,
                    metadata=metadata,
                    name=suite_name,
                    perfherder=perfherder
                )
                KNOWN_PERFHERDER_TESTS.append(suite_name)

        # UPDATE metadata PROPERTIES TO BETTER VALUES
        metadata.run.timestamp = coalesce(perfherder.testrun.date, metadata.run.timestamp, metadata.action.timestamp, metadata.action.start_time)
        metadata.result.suite = metadata.run.suite = suite_name
        metadata.result.framework = metadata.run.framework = perfherder.framework
        metadata.result.extraOptions = perfherder.extraOptions

        mainthread_transform(perfherder.results_aux)
        mainthread_transform(perfherder.results_xperf)

        new_records = FlatList()

        # RECORD THE UNKNOWN PART OF THE TEST RESULTS
        if perfherder.keys() - KNOWN_PERFHERDER_PROPERTIES:
            remainder = copy(perfherder)
            for k in KNOWN_PERFHERDER_PROPERTIES:
                remainder[k] = None
            if any(remainder.values()):
                new_records.append(set_default(remainder, metadata))

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
                                    "test": text_type(subtest.name) + "." + text_type(g),
                                    "ordering": i,
                                    "unit": subtest.unit,
                                    "lower_is_better": subtest.lowerIsBetter
                                }
                            )},
                            metadata
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
                                "lower_is_better": subtest.lowerIsBetter,
                                "raw_replicates": subtest.ref_replicates,
                                "control_replicates": subtest.base_replicates,
                                "value": samples[0] if len(samples) == 1 else None
                            }
                        )},
                        metadata
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
                                    "test": text_type(test_name) + "." + text_type(g),
                                    "ordering": i
                                }
                            )},
                            metadata
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
                        metadata
                    )
                    new_records.append(new_record)
                    total.append(new_record.result.stats)
        elif perfherder.value != None:  # SUITE CAN HAVE A SINGLE VALUE, AND NO SUB-TESTS
            new_record = set_default(
                {"result": set_default(
                    stats(source_key, [perfherder.value], None, suite_name),
                    {
                        "unit": perfherder.unit,
                        "lower_is_better": perfherder.lowerIsBetter,
                        "value": perfherder.value
                    }
                )},
                metadata
            )
            new_records.append(new_record)
            total.append(new_record.result.stats)
        elif perfherder.is_empty:
            metadata.run.result.is_empty = True
            new_records.append(metadata)
            pass
        else:
            new_records.append(metadata)
            Log.warning(
                "While processing {{uid}}, no `results` or `subtests` found in {{name|quote}}",
                uid=source_key,
                name=suite_name
            )

        # ADD RECORD FOR GEOMETRIC MEAN SUMMARY
        metadata.run.stats = geo_mean(total)
        Log.note(
            "Done {{uid}}, processed {{framework|upper}} :: {{name}}, transformed {{num}} records",
            uid=source_key,
            framework=framework_name,
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

        rejects = unwraplist([text_type(v) for v in given_values if Math.is_nan(v) or not Math.is_finite(v)])
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


KNOWN_PERFHERDER_OPTIONS = ["pgo", "e10s", "stylo", "coverage"]
KNOWN_PERFHERDER_PROPERTIES = {"_id", "etl", "extraOptions", "framework", "is_empty", "lowerIsBetter", "name", "pulse", "results", "talos_counters", "test_build", "test_machine", "testrun", "shouldAlert", "subtests", "summary", "value"}
KNOWN_PERFHERDER_TESTS = [
    # BE SURE TO PUT THE LONGEST STRINGS FIRST
    "about_preferences_basic",
    "ares6-sm",
    "ARES6",
    "a11yr",
    "avcodec section sizes",
    "avutil section sizes",
    "Base Content Explicit",
    "Base Content Heap Unclassified",
    "Base Content JS",
    "Base Content Resident Unique Memory",
    "basic_compositor_video",
    "BenchCollections",
    "bloom_basic_ref",
    "bloom_basic_singleton",
    "bloom_basic",
    "build times",
    "cart",
    "chromez",
    "chrome",
    "clone_errored",  # vcs
    "clone",   # vcs
    "compiler_metrics",
    "compiler warnings",
    "cpstartup",
    "damp",
    "displaylist_mutate",
    "dromaeo_css",
    "dromaeo_dom",
    "dromaeojs",
    "Explicit Memory",
    "flex",
    "GfxBench",
    "g1",
    "g2",
    "g3",
    "g4-disabled",
    "g4",
    "g5",
    "glterrain",
    "glvideo",
    "h1",
    "h2",
    "Heap Unclassified",
    "Images",
    "installer size",
    "JetStream",
    "jittest.jittest.overall",
    "JS",
    "kraken",
    "NSPR section sizes",
    "NSS section sizes",
    "media_tests",
    "mochitest-browser-chrome-screenshots",
    "mochitest-browser-chrome",
    "motionmark_animometer",
    "motionmark_htmlsuite",
    "motionmark, transformed",
    "motionmark",
    "octane-sm",
    "other_nol64",
    "other_l64",
    "other",
    "overall",  # VCS
    "perf_reftest_singletons",
    "perf_reftest",  # THIS ONE HAS THE COMPARISION RESULTS
    "pull",  # VCS
    "purge",  # VCS
    "Quantum_1",
    "quantum_pageload_amazon",
    "quantum_pageload_facebook",
    "quantum_pageload_google",
    "quantum_pageload_youtube",
    "raptor-assorted-dom-firefox",
    "raptor-assorted-dom-chrome",
    "raptor-firefox-tp6-amazon",
    "raptor-firefox-tp6-facebook",
    "raptor-firefox-tp6-google",
    "raptor-firefox-tp6-youtube",
    "raptor-google-docs-firefox",
    "raptor-google-docs-chrome",
    "raptor-google-sheets-firefox",
    "raptor-google-sheets-chrome",
    "raptor-google-slides-firefox",
    "raptor-google-slides-chrome",
    "raptor-motionmark-animometer-firefox",
    "raptor-motionmark-animometer-chrome",
    "raptor-motionmark-htmlsuite-firefox",
    "raptor-motionmark-htmlsuite-chrome",
    "raptor-unity-webgl-firefox",
    "raptor-unity-webgl-chrome",
    "raptor-speedometer-firefox",
    "raptor-speedometer-chrome",
    "raptor-stylebench-firefox",
    "raptor-stylebench-chrome",
    "raptor-sunspider-firefox",
    "raptor-sunspider-chrome",
    "raptor-tp6-amazon-firefox",
    "raptor-tp6-amazon-chrome",
    "raptor-tp6-facebook-firefox",
    "raptor-tp6-facebook-chrome",
    "raptor-tp6-google-firefox",
    "raptor-tp6-google-chrome",
    "raptor-tp6-youtube-firefox",
    "raptor-tp6-youtube-chrome",
    "raptor-wasm-misc-baseline-firefox",
    "raptor-wasm-misc-chrome"
    "raptor-wasm-misc-firefox"
    "raptor-wasm-misc-ion-firefox",
    "raptor-wasm-misc-ion-chrome",
    "raptor-webaudio-firefox",
    "raptor-webaudio-chrome",
    "rasterflood_gradient",
    "rasterflood_svg",
    "removed_missing_shared_store",
    "Resident Memory",
    "sccache cache_write_errors",
    "sccache hit rate",
    "sccache requests_not_cacheable",
    "sessionrestore_many_windows",
    "sessionrestore_no_auto_restore",
    "sessionrestore",
    "six-speed",
    "six-speed-sm",
    "sparse_update_config",  # VCS
    "speedometer",
    "Strings",
    "stylebench",
    "Stylo",
    "sunspider",
    "sunspider-sm",  # sm = spidermonkey
    "sunspider-v8",
    "svgr-disabled",
    "svgr",
    "tabpaint",
    "tart_flex",
    "tart",
    "TestStandardURL",
    "TreeTraversal",
    "tcanvasmark",
    "tcheck2",
    "tp4m_nochrome",
    "tp4m",
    "tp5n",
    "tp5o_multiwindow_4_singlecp",
    "tp5o_scroll",
    "tp5o_webext",
    "tp5o",
    "tp6_amazon_heavy",
    "tp6_amazon",
    "tp6_facebook_heavy",
    "tp6_facebook",
    "tp6_google_heavy",
    "tp6_google",
    "tp6_youtube_heavy",
    "tp6_youtube",
    "tp6",
    "tp6-stylo-threads",
    "tpaint",
    "tps",
    "tresize",
    "trobocheck2",
    "ts_paint_webext",
    "ts_paint_heavy",
    "ts_paint_flex",
    "ts_paint",
    "tscrollx",
    "tsvgr_opacity",
    "tsvg_static",
    "tsvgx",
    "update_sparse",  #VCS
    "update",  # VCS
    "v8_7",
    "web-tooling-benchmark-sm",
    "web-tooling-benchmark-v8",
    "xperf",
    "XUL section sizes"
]
