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
from testlog_etl.transforms import git_revision
from testlog_etl.transforms.pulse_block_to_es import transform_buildbot


DEBUG = True


def process_unittest_in_s3(source_key, source, destination, please_stop=None):
    lines = source.read_lines()

    etl_header = convert.json2value(lines[0])

    # FIX ETL IDS
    e = etl_header
    while e:
        if isinstance(e.id, basestring):
            e.id = int(e.id.split(":")[0])
        e = e.source

    bb_summary = transform_buildbot(convert.json2value(lines[1]))
    unittest_log = lines[2:]
    return process_unittest(source_key, etl_header, bb_summary, unittest_log, destination, please_stop=please_stop)


def process_unittest(source_key, etl_header, buildbot_summary, unittest_log, destination, please_stop=None):

    timer = Timer("Process log {{file}} for {{key}}", {
        "file": etl_header.name,
        "key": source_key
    })
    try:
        with timer:
            summary = accumulate_logs(source_key, etl_header.name, unittest_log)
    except Exception, e:
        Log.error("Problem processing {{key}}", {"key": source_key}, e)
        raise e

    buildbot_summary.etl = {
        "id": 0,
        "name": "unittest",
        "timestamp": Date.now().unix,
        "source": etl_header,
        "type": "join",
        "revision": git_revision,
        "duration": timer.duration.seconds
    }
    buildbot_summary.run.stats = summary.stats
    buildbot_summary.run.stats.duration = summary.stats.end_time - summary.stats.start_time

    if DEBUG:
        age = Date.now() - Date(buildbot_summary.run.stats.start_time * 1000)
        if age > Duration.DAY:
            Log.alert("Test is {{days|round(decimal=1)}} days old", {"days": age / Duration.DAY})
        Log.note("Done\n{{data|indent}}", {"data": buildbot_summary.run.stats})

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
                        "etl": {"id":i}
                    },
                    buildbot_summary
                )
            })
    destination.extend(new_data)
    return new_keys


def accumulate_logs(source_key, file_name, lines):
    accumulator = LogSummary()
    for line in lines:
        accumulator.stats.bytes += len(line) + 1  # INCLUDE THE \n THAT WOULD HAVE BEEN AT END OF EACH LINE
        line = strings.strip(line)

        if line == "":
            continue
        try:
            accumulator.stats.lines += 1
            log = convert.json2value(line)
            log.time = log.time / 1000
            accumulator.stats.start_time = Math.min(accumulator.stats.start_time, log.time)
            accumulator.stats.end_time = Math.max(accumulator.stats.end_time, log.time)

            # FIX log.test TO BE A STRING
            if isinstance(log.test, list):
                log.test = " ".join(log.test)

            accumulator.__getattribute__(log.action)(log)
        except Exception, e:
            accumulator.stats.bad_lines += 1

    output = accumulator.summary()
    Log.note("{{num_bytes|comma}} bytes, {{num_lines|comma}} lines and {{num_tests|comma}} tests in {{name}} for key {{key}}", {
        "key":source_key,
        "num_bytes": output.stats.bytes,
        "num_lines": output.stats.lines,
        "num_tests": output.stats.total,
        "bad_lines": output.stats.bad_lines,
        "name": file_name
    })
    return output


class LogSummary(Dict):
    def __init__(self):
        Dict.__init__(self)
        self.tests = Dict()

    def suite_start(self, log):
        pass

    def test_start(self, log):
        if isinstance(log.test, list):
            log.test = " ".join(log.test)
        self.tests[literal_field(log.test)] = Dict(
            test=log.test,
            start_time=log.time
        )

    def test_status(self, log):
        self.stats.test_status_lines += 1
        if not log.test:
            Log.error("log has blank 'test' property! Do not know how to handle.")

        test = self.tests[literal_field(log.test)]
        test.stats.test_status_lines += 1
        if not test:
            self.tests[literal_field(log.test)] = test = Dict(
                test=log.test,
                start_time=log.time,
                missing_test_start=True
            )
        test.last_log_time = log.time
        test.stats[log.status.lower()] += 1

    def process_output(self, log):
        pass

    def log(self, log):
        self.stats.log_lines += 1
        if not log.test:
            return

        test = self.tests[literal_field(log.test)]
        test.stats.log_lines += 1
        if not test:
            self.tests[literal_field(log.test)] = test = wrap({
                "test": log.test,
                "start_time": log.time,
                "missing_test_start": True,
            })
        test.last_log_time = log.time
        test.stats.log_lines += 1

    def crash(self, log):
        self.stats.crash_lines += 1
        if not log.test:
            return

        test = self.tests[literal_field(log.test)]
        test.stats.crash_lines += 1
        if not test:
            self.tests[literal_field(log.test)] = test = Dict(
                test=log.test,
                start_time=log.time,
                crash=True,
                missing_test_start=True
            )
        test.last_log_time = log.time

    def test_end(self, log):
        test = self.tests[literal_field(log.test)]
        if not test:
            self.tests[literal_field(log.test)] = test = Dict(
                test=log.test,
                start_time=log.time,
                missing_test_start=True
            )

        test.ok = not log.expected
        test.result = log.status
        test.expected = nvl(log.expected, log.status)
        test.end_time = log.time
        test.duration = nvl(test.end_time - test.start_time, log.extra.runtime)
        test.extra = test.extra

    def suite_end(self, log):
        pass

    def summary(self):
        self.tests = tests = wrap(list(self.tests.values()))

        for t in tests:
            if not t.result:
                t.result = "NONE"
                t.end_time = t.last_log_time
                t.duration = t.end_time - t.start_time
                t.missing_test_end = True

        self.stats.total = len(tests)
        self.stats.ok = len([t for t in tests if t.ok])
        # COUNT THE NUMBER OF EACH RESULT
        try:
            for r in set(tests.select("result")):
                self.stats[r.lower()] = len([t for t in tests if t.result == r])
        except Exception, e:
            Log.error("problem", e)

        return self

