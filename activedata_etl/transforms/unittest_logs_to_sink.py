# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division, unicode_literals

from mo_dots.lists import last

from activedata_etl.transforms import TRY_AGAIN_LATER
from activedata_etl.transforms.pulse_block_to_es import transform_buildbot
from jx_python import jx
from mo_dots import Data, Null, coalesce, set_default, wrap, listwrap
from mo_future import text, is_text
from mo_json import json2value, scrub
from mo_logs import Log, machine_metadata, strings
from mo_logs.exceptions import Except
from mo_math import MAX, MIN
from mo_times.dates import Date
from mo_times.durations import DAY
from mo_times.timer import Timer
from pyLibrary.env import git
from mo_times.dates import parse

DEBUG = True
ACCESS_DENIED = "Access Denied to {{url}} in {{key}}"


def process_unittest_in_s3(source_key, source, destination, resources, please_stop=None):
    lines = source.read_lines()
    etl_header = json2value(lines[0]).etl
    bb_summary = transform_buildbot(json2value(lines[1]), resources=resources, source_key=source_key)
    return process_unittest(source_key, etl_header, bb_summary, lines, destination, please_stop=please_stop)


def process_unittest(source_key, etl_header, buildbot_summary, unittest_log, destination, please_stop=None):
    """
    :param source_key: THE PARENT PATH KEY
    :param etl_header: THE PARENT ETL STRUCTURE
    :param buildbot_summary: STRUCTURE TO ANNOTATE WITH TEST RESULTS
    :param unittest_log: GENERATOR OF LINES WITH STRUCTURED LOG ENTRIES
    :param destination: S3 BUCKET TO PUT THE RESULTS
    :param please_stop:CHECK OFTEN TO EXIT FAST
    :return: KEYS FOR ALL TEST RESULTS
    """

    timer = Timer("Process log {{url}} for {{key}}", {
        "url": etl_header.url,
        "key": source_key
    })
    try:
        with timer:
            summary = accumulate_logs(
                source_key,
                etl_header.url,
                unittest_log,
                buildbot_summary.run.suite.name,
                please_stop
            )
    except Exception as e:
        e = Except.wrap(e)
        if "EOF occurred in violation of protocol" in e:
            raise Log.error(TRY_AGAIN_LATER, reason="EOF ssl violation")
        elif ACCESS_DENIED in e and buildbot_summary.task.state in ["failed", "exception"]:
            summary = Null
        elif ACCESS_DENIED in e:
            summary = Null
            Log.warning("Problem processing {{key}}", key=source_key, cause=e)
        else:
            raise Log.error("Problem processing {{key}} after {{duration|round(decimal=0)}}seconds", key=source_key, duration=timer.duration.seconds, cause=e)

    buildbot_summary.etl = {
        "id": 0,
        "name": "unittest",
        "timestamp": Date.now().unix,
        "source": etl_header,
        "type": "join",
        "revision": git.get_revision(),
        "machine": machine_metadata,
        "duration": timer.duration
    }
    buildbot_summary.run.stats = summary.stats
    buildbot_summary.run.stats.duration = summary.stats.end_time - summary.stats.start_time
    buildbot_summary.run.suite.groups = summary.groups

    if DEBUG:
        age = Date.now() - Date(buildbot_summary.run.stats.start_time)
        if age > DAY:
            Log.alert("Test is {{days|round(decimal=1)}} days old", days=age / DAY)
        Log.note("Done\n{{data|indent}}", data=buildbot_summary.run.stats)

    new_keys = []
    new_data = []

    if not summary.tests:
        key = source_key + ".0"
        new_keys.append(key)

        new_data.append({
            "id": key,
            "value": buildbot_summary
        })
    else:
        for i, t in enumerate(summary.tests):
            key = source_key + "." + text(i)
            new_keys.append(key)

            new_data.append({
                "id": key,
                "value": set_default(
                    {
                        "result": t,
                        "etl": {"id": i}
                    },
                    buildbot_summary
                )
            })
    destination.extend(new_data)
    return new_keys


def accumulate_logs(source_key, url, lines, suite_name, please_stop):
    accumulator = LogSummary(source_key, url)
    last_line_was_json = True
    for line_num, line in enumerate(lines):
        if please_stop:
            Log.error("Shutdown detected.  Structured log iterator is stopped.")
        accumulator.stats.bytes += len(line) + 1  # INCLUDE THE \n THAT WOULD HAVE BEEN AT END OF EACH LINE
        line = strings.strip(line)

        if line == "":
            continue
        try:
            accumulator.stats.lines += 1
            last_line_was_json = False
            log = json2value(line)
            last_line_was_json = True
            log.time = parse(log.time)
            accumulator.stats.start_time = MIN([accumulator.stats.start_time, log.time])
            accumulator.stats.end_time = MAX([accumulator.stats.end_time, log.time])

            # FIX log.test TO BE A STRING
            if isinstance(log.test, list):
                log.test = " ".join(log.test)

            accumulator.stats.action[log.action] += 1
            try:
                getattr(accumulator, log.action)(log)
            except AttributeError:
                pass

            if log.subtest:
                accumulator.end_time = log.time
        except Exception as e:
            e = Except.wrap(e)
            if line.startswith('<!DOCTYPE html>') or line.startswith('<?xml version="1.0"'):
                content = "\n".join(lines)
                if "<Code>AccessDenied</Code>" in content:
                    Log.error(ACCESS_DENIED, url=accumulator.url, key=source_key)
                else:
                    Log.error(TRY_AGAIN_LATER, reason="Remote content is not ready")
            prefix = strings.limit(line, 500)
            Log.warning(
                "bad line #{{line_number}} in key={{key}} url={{url|quote}}:\n{{line|quote}}",
                key=source_key,
                line_number=accumulator.stats.lines,
                line=prefix,
                url=url,
                cause=e
            )
            accumulator.stats.bad_lines += 1

    if not last_line_was_json:
        # HAPPENS WHEN FILE IS DOWNLOADED TOO SOON, AND IS INCOMPLETE
        Log.error(TRY_AGAIN_LATER, reason="Incomplete file")

    output = accumulator.summary()
    Log.note(
        "{{num_bytes|comma}} bytes, {{num_lines|comma}} lines and {{num_tests|comma}} tests in {{url|quote}} for key {{key}}",
        key=source_key,
        num_bytes=output.stats.bytes,
        num_lines=output.stats.lines,
        num_tests=output.stats.total,
        bad_lines=output.stats.bad_lines,
        url=url
    )
    return output


class LogSummary(object):
    def __init__(self, source_key, url):
        self.source_key = source_key
        self.url = url
        self.suite_name = None
        self.start_time = None
        self.end_time = None
        self.tests = {}
        self.logs = {}
        self.stats = Data()
        self.groups = None
        self.test_to_group = {}   # MAP FROM TEST NAME TO GROUP NAME

    def suite_start(self, log):
        self.suite_name = log.name
        self.start_time = log.time
        for k, v in log.items():
            if k in KNOWN_SUITE_PROPERTIES:
                k = fix_suite_property_name(k)
                setattr(self, k, v)
            elif k == "tests":
                # EXPECTING A DICT OF LISTS
                try:
                    if v:
                        for group, tests in v.items():
                            if group == "default":
                                continue
                            for test in tests:
                                self.test_to_group[test] = group
                        self.groups = jx.sort(set(v.keys()) - {"default"})
                except Exception as e:
                    Log.warning(
                        "can not process the suite_start.tests dictionary for {{key}}\n{{example|json|indent}}",
                        example=v,
                        key=self.source_key,
                        cause=e
                    )
            elif k in ["action", "time", "name"]:
                pass
            else:
                KNOWN_SUITE_PROPERTIES.add(k)
                Log.warning("do not know about new suite property {{name|quote}} in {{key}} ", name=k, key=self.source_key)

    def test_start(self, log):
        if isinstance(log.test, list):
            log.test = " ".join(log.test)
        test = Data(
            test=log.test,
            start_time=log.time,
            group=self.test_to_group.get(log.test)
        )
        for k,v in log.items():
            if k in KNOWN_TEST_PROPERTIES:
                if v != None and v != "":
                    test[k] = v
            elif k in ["action", "test", "time"]:
                pass
            else:
                KNOWN_TEST_PROPERTIES.add(k)
                Log.warning("do not know about new test property {{name|quote}} in {{key}} ", name=k, key=self.source_key)

        tests = self.tests.setdefault(log.test, [])
        tests.append(test)
        self.end_time = log.time


    def test_status(self, log):
        if not log.test:
            # {
            #     "status": "PASS",
            #     "thread": "None",
            #     "subtest": "Background event should be on background thread",
            #     "pid": null,
            #     "source": "robocop",
            #     "test": "",
            #     "time": 1450098827133,
            #     "action": "test_status",
            #     "message": ""
            # }
            Log.warning("Log has blank 'test' property! Do not know how to handle. In {{key}} ", key=self.source_key)
            return
        self.logs.setdefault(log.test, []).append(log)
        test = self._get_test(log)
        test.stats.action.test_status += 1
        test.end_time = log.time
        test.stats[log.status.lower()] += 1

        if log.subtest:
            ok = log.expected == None or log.expected == log.status

            if not ok:
                # WE CAN NOT AFFORD TO STORE ALL SUBTESTS, ONLY THE FAILURES
                if test.subtests:
                    last_test = last(test.subtests)
                    if last_test.name == log.subtest:
                        last_test.repeat += 1
                        return

                message = scrub(log.message)
                if is_text(message):
                    message = strings.limit(message, 6000)

                ki = set(i for i in listwrap(log.known_intermittent))
                test.subtests += [{
                    "name": log.subtest,
                    "subtest": log.subtest,
                    "ok": ok,
                    "ok_intermittent": ok or log.status in ki,
                    "known_intermittent": ki,
                    "status": log.status,
                    "expected": log.expected,
                    "timestamp": log.time,
                    "message": message,
                    "ordering": len(test.subtests)
                }]

    def process_output(self, log):
        if log.test:
            self.logs.setdefault(log.test, []).append(log)
        pass

    def log(self, log):
        if not log.test:
            return

        self.logs.setdefault(log.test, []).append(log)
        test = self._get_test(log)
        test.stats.action.log += 1
        test.end_time = log.time
        test.stats.action.log += 1

    def crash(self, log):
        if not log.test:
            log.test = "!!SUITE CRASH!!"

        self.logs.setdefault(log.test, []).append(log)

        test = self._get_test(log)
        test.ok = False
        test.crash=True,

        test.status = log.status
        test.end_time = log.time
        test.missing_test_end = True

        #RECORD THE CRASH RESULTS
        # test.crash_result = log.copy()
        # test.crash_result.action = None

    def test_end(self, log):
        self.logs.setdefault(log.test, []).append(log)
        test = self._get_test(log)

        test.ok = log.expected == None or log.expected == log.status
        test.known_intermittent = ki = set(i for i in listwrap(log.known_intermittent))
        test.ok_intermittent = test.ok or log.status in ki

        if not all(test.subtests.ok):
            test.ok = False
        if not all(test.subtests.ok_intermittent):
            test.ok_intermittent = False

        test.status = log.status
        test.expected = coalesce(log.expected, log.status)
        test.end_time = log.time
        test.duration = coalesce(test.end_time - test.start_time, log.extra.runtime)
        test.extra = test.extra

    def _get_test(self, log):
        test = last(self.tests.get(log.test))
        if not test:
            test = Data(
                test=log.test,
                start_time=log.time,
                missing_test_start=True
            )
            self.tests[log.test] = [test]
        return test

    def suite_end(self, log):
        pass

    def summary(self):
        self.tests = tests = wrap([vv for v in self.tests.values() for vv in v])

        for t in tests:
            t.duration = t.end_time - t.start_time
            if t.status:
                continue

            t.ok = False

            t.missing_test_end = True

        self.stats.total = len(tests)
        # COUNT THE NUMBER OF EACH RESULT
        for i, t in enumerate(tests):
            try:
                if t.status:
                    self.stats.status[t.status.lower()] += 1
            except Exception as e:
                Log.warning("problem with key {{key}} on item {{i}}", key=self.source_key, i=i, cause=e)
                break

        self.stats.ok = sum(1 for t in tests if t.ok)
        self.test_to_group = None  # REMOVED
        return self


def fix_suite_property_name(k):
    if k == "runinfo":
        return "run_info"
    return k


KNOWN_SUITE_PROPERTIES = {
    "component",
    "device_info",
    "extra",
    "pid",
    "run_info",
    "runinfo",
    "source",
    "thread",
    "version_info",
}

KNOWN_TEST_PROPERTIES = {
    "component",
    "jitflags",
    "js_source",
    "pid",
    "source",
    "thread",
}
