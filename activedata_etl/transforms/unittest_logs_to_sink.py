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

from activedata_etl.transforms import TRY_AGAIN_LATER
from activedata_etl.transforms.pulse_block_to_es import transform_buildbot
from pyLibrary import convert, strings
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, wrap, coalesce, set_default, literal_field
from pyLibrary.env.git import get_git_revision
from pyLibrary.jsons import scrub
from pyLibrary.maths import Math
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY
from pyLibrary.times.timer import Timer

DEBUG = True


def process_unittest_in_s3(source_key, source, destination, resources, please_stop=None):
    lines = source.read_lines()

    etl_header = convert.json2value(lines[0])

    # FIX ETL IDS
    e = etl_header
    while e:
        if isinstance(e.id, basestring):
            e.id = int(e.id.split(":")[0])
        e = e.source

    bb_summary = transform_buildbot(convert.json2value(lines[1]), resources=resources, source_key=source_key)
    unittest_log = lines[2:]
    return process_unittest(source_key, etl_header, bb_summary, unittest_log, destination, please_stop=please_stop)


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
            summary = accumulate_logs(source_key, etl_header.url, unittest_log, please_stop)
    except Exception, e:
        Log.error("Problem processing {{key}} after {{duration|round(decimal=0)}}seconds", key=source_key, duration=timer.duration.seconds, cause=e)
        summary = None

    buildbot_summary.etl = {
        "id": 0,
        "name": "unittest",
        "timestamp": Date.now().unix,
        "source": etl_header,
        "type": "join",
        "revision": get_git_revision(),
        "duration": timer.duration
    }
    buildbot_summary.run.stats = summary.stats
    buildbot_summary.run.stats.duration = summary.stats.end_time - summary.stats.start_time

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
            key = source_key + "." + unicode(i)
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


def accumulate_logs(source_key, url, lines, please_stop):
    accumulator = LogSummary(url)
    last_line_was_json = True
    for line in lines:
        if please_stop:
            Log.error("Shutdown detected.  Structured log iterator is stopped.")
        accumulator.stats.bytes += len(line) + 1  # INCLUDE THE \n THAT WOULD HAVE BEEN AT END OF EACH LINE
        line = strings.strip(line)

        if line == "":
            continue
        try:
            accumulator.stats.lines += 1
            last_line_was_json = False
            log = convert.json2value(line)
            last_line_was_json = True
            log.time = log.time / 1000
            accumulator.stats.start_time = Math.min(accumulator.stats.start_time, log.time)
            accumulator.stats.end_time = Math.max(accumulator.stats.end_time, log.time)

            # FIX log.test TO BE A STRING
            if isinstance(log.test, list):
                log.test = " ".join(log.test)

            try:
                accumulator.__getattribute__(log.action)(log)
            except AttributeError:
                accumulator.stats.action[log.action] += 1

            if log.subtest:
                accumulator.last_subtest = log.time
        except Exception, e:
            e= Except.wrap(e)
            if "Can not decode JSON" in e:
                Log.error(TRY_AGAIN_LATER, reason="Bad JSON")
            elif line.startswith('<!DOCTYPE html>') or line.startswith('<?xml version="1.0"'):
                Log.error(TRY_AGAIN_LATER, reason="Log is not ready")

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


class LogSummary(Dict):
    def __init__(self, url):
        Dict.__init__(self)
        self.tests = Dict()
        self.logs = Dict()
        self.last_subtest = None
        self.url = url

    def suite_start(self, log):
        pass

    def test_start(self, log):
        if isinstance(log.test, list):
            log.test = " ".join(log.test)
        self.tests[literal_field(log.test)] = Dict(
            test=log.test,
            start_time=log.time
        )
        self.last_subtest=log.time

    def test_status(self, log):
        self.stats.action.test_status += 1
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
            Log.warning("Log has blank 'test' property! Do not know how to handle. In {{url}}", url=self.url)
            return

        self.logs[literal_field(log.test)] += [log]
        test = self.tests[literal_field(log.test)]
        test.stats.action.test_status += 1
        if not test:
            self.tests[literal_field(log.test)] = test = Dict(
                test=log.test,
                start_time=log.time,
                missing_test_start=True
            )
        test.last_log_time = log.time
        test.stats[log.status.lower()] += 1

        if log.subtest:
            ok = True if log.expected == None or log.expected == log.status else False
            if not ok:
                if test.subtests:
                    last = test.subtests.last()
                    if last.name == log.subtest:
                        last.repeat += 1
                        return

                # WE CAN NOT AFFORD TO STORE ALL SUBTESTS, ONLY THE FAILURES
                test.subtests += [{
                    "name": log.subtest,
                    "subtest": log.subtest,
                    "ok": ok,
                    "status": log.status.lower(),
                    "expected": log.expected.lower(),
                    "timestamp": log.time,
                    "message": scrub(log.message),
                    "ordering": len(test.subtests)
                }]

    def process_output(self, log):
        self.stats.action.process_output += 1
        if log.test:
            self.logs[literal_field(log.test)] += [log]
        pass

    def log(self, log):
        self.stats.action.log += 1
        if not log.test:
            return

        self.logs[literal_field(log.test)] += [log]
        test = self.tests[literal_field(log.test)]
        test.stats.action.log += 1
        if not test:
            self.tests[literal_field(log.test)] = test = wrap({
                "test": log.test,
                "start_time": log.time,
                "missing_test_start": True,
            })
        test.last_log_time = log.time
        test.stats.action.log += 1

    def crash(self, log):
        self.stats.action.crash += 1
        if not log.test:
            test_name = "!!SUITE CRASH!!"
        else:
            test_name = literal_field(log.test)

        self.logs[test_name] += [log]
        test = self.tests[test_name]
        if not test:
            self.tests[test_name] = test = Dict(
                test=log.test,
                start_time=log.time,
                crash=True,
                missing_test_start=True
            )

        test.ok = False
        test.result = log.status   #TODO: REMOVE ME AFTER November 2015
        test.status = log.status
        test.last_log_time = log.time
        test.missing_test_end = True

        #RECORD THE CRASH RESULTS
        # test.crash_result = log.copy()
        # test.crash_result.action = None

    def test_end(self, log):
        self.logs[literal_field(log.test)] += [log]
        test = self.tests[literal_field(log.test)]
        if not test:
            self.tests[literal_field(log.test)] = test = Dict(
                test=log.test,
                start_time=log.time,
                missing_test_start=True
            )

        test.ok = True if log.expected == None or log.expected == log.status else False
        if not all(test.subtests.ok):
            test.ok = False
        test.result = log.status   #TODO: REMOVE ME AFTER November 2015
        test.status = log.status
        test.expected = coalesce(log.expected, log.status)
        test.end_time = log.time
        test.duration = coalesce(test.end_time - test.start_time, log.extra.runtime)
        test.extra = test.extra

    def suite_end(self, log):
        pass

    def summary(self):
        self.tests = tests = wrap(list(self.tests.values()))

        for t in tests:
            if t.status:
                continue

            t.result = "NONE"  #TODO Remove November 2015
            t.status = "NONE"  #TODO Remove November 2015
            t.ok = False
            t.end_time = t.last_log_time
            t.duration = t.end_time - t.start_time
            t.missing_test_end = True

        self.stats.total = len(tests)
        # COUNT THE NUMBER OF EACH RESULT
        try:
            for t in tests:
                self.stats.status[t.status.lower()] += 1
        except Exception, e:
            Log.error("problem", e)

        self.stats.ok = sum(1 for t in tests if t.ok)
        return self
