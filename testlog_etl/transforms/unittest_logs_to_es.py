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
from pyLibrary.queries import qb
from pyLibrary.dot import Dict, wrap, nvl, set_default, literal_field
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key


DEBUG = True


def process_unittest(source_key, source, destination):
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
        "key":source_key
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
        "duration": timer.duration
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


def process_unittest_log(source_key, file_name, lines):
    accumulator = LogSummary()
    for line in lines:
        accumulator.stats.bytes += len(line) + 1  # INCLUDE THE \n THAT WOULD HAVE BEEN AT END OF EACH LINE
        line = strings.strip(line)

        if line == "":
            continue
        try:
            accumulator.stats.lines += 1
            log = convert.json2value(line)
            log.time = log.time/1000
            accumulator.stats.start_time = Math.min(accumulator.stats.start_time, log.time)
            accumulator.stats.end_time = Math.max(accumulator.stats.end_time, log.time)


            # FIX log.test TO BE A STRING
            if isinstance(log.test, list):
                log.test = " ".join(log.test)

            accumulator.__getattribute__(log.action)(log)

        except Exception, e:
            accumulator.stats.bad_lines += 1
            if len(line.split("=")) == 2:  # TODO: REMOVE THIS CHECK
                # SUPRESS THESE WARNINGS FOR NOW, OLD ETL LEAKED NON-JSON DOCUMENTS
                # StartTime=1409123984798
                # CrashTime=1498346728
                pass
            else:
                Log.warning("Problem with line while processing {{key}}. Ignored.\n{{line|indent}}", {"key": source_key, "line": line}, e)

    output = accumulator.summary()
    Log.note("{{num_bytes|comma}} bytes, {{num_lines|comma}} lines and {{num_tests|comma}} tests in {{name}} for key {{key}}", {
        "key":source_key,
        "num_bytes": output.stats.bytes,
        "num_lines": output.stats.lines,
        "num_tests": output.stats.total,
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
        if not log.test:
            Log.error("log has blank 'test' property! Do not know how to hanlde.")

        test = self.tests[literal_field(log.test)]
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
        if not log.test:
            return

        test = self.tests[literal_field(log.test)]
        if not test:
            self.tests[literal_field(log.test)] = test = Dict(
                test=log.test,
                start_time=log.time,
                missing_test_start=True
            )
        test.last_log_time = log.time
        test.stats.log_lines += 1

    def crash(self, log):
        if not log.test:
            return

        test = self.tests[literal_field(log.test)]
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

        if not test.ok:
            Log.note("Bad test {{result}}", {"result": test})


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


def transform_buildbot(payload):
    output = qb.select_one(payload, [
        {"name": "run.files", "value": "blobber_files"},
        {"name": "build.date", "value": "builddate"},
        {"name": "build.name", "value": "buildername"},
        {"name": "build.id", "value": "buildid"},
        {"name": "build.type", "value": "buildtype"},
        {"name": "build.url", "value": "buildurl"},
        {"name": "run.insertion_time", "value": "insertion_time"},
        {"name": "run.job_number", "value": "job_number"},
        {"name": "run.key", "value": "key"},
        {"name": "build.locale", "value": "locale"},
        {"name": "run.logurl", "value": "logurl"},
        {"name": "machine.os", "value": "os"},
        {"name": "machine.platform", "value": "platform"},
        {"name": "build.product", "value": "product"},
        {"name": "build.release", "value": "release"},
        {"name": "build.revision", "value": "revision"},
        {"name": "machine.name", "value": "slave"},
        {"name": "run.status", "value": "status"},
        {"name": "run.talos", "value": "talos"},
        {"name": "run.suite", "value": "test"},
        {"name": "run.timestamp", "value": "timestamp"},
        {"name": "build.branch", "value": "tree"},
    ])

    path = output.run.suite.split("-")
    if Math.is_integer(path[-1]):
        output.run.chunk = int(path[-1])
        output.run.suite = "-".join(path[:-1])

    output.run.timestamp = Date(output.run.timestamp).unix

    output.run.files = [{"name": name, "url":url} for name, url in output.run.files.items()]

    return output
