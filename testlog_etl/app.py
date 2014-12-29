# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
import functools

import requests

from pyLibrary import convert
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.env.pulse import Pulse
from pyLibrary.maths import Math
from pyLibrary.queries import Q
from pyLibrary.structs import wrap, set_default, Struct, nvl
from pyLibrary.thread.threads import Thread, Queue
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer



def process_unittest(envelope, sink):
    data = transform_buildbot(envelope.payload)
    Log.note("{{data}}", {"data": data})

    found_log = False
    all_tests = []
    for name, url in data.run.files.items():
        try:
            if "structured" in name and name.endswith(".log"):
                found_log = True
                with Timer("Process log {{file}}", {"file": name}):
                    results = process_log(name, url)
                data.run.counts = results.counts
                all_tests.append(results.tests)
        except Exception, e:
            Log.error("Problem processing {{url}}", {"url": url}, e)

    if found_log:
        Log.note("NO STRUCTURED LOG")
        return

    data.etl = {
        "name": "unittest",
        "timestamp": Date.now().milli,
        "source": data.etl,
        "type": "join"
    }

    sink.write("\n".join([
        set_default(
            {
                "result": t,
                "etl": {
                    "id": i
                }
            },
            data
        )
        for i, t in enumerate(all_tests)
    ]))


def process_log(name, url):
    accumulator = LogSummary()
    accumulator.url = url
    accumulator.counts.lines = 0

    if url:
        response = requests.get(url)
    else:
        response = Struct(content="")  # DUMMY CONTENT FOR null URL


    for line in response.content.split("\n"):
        if line.strip() == "":
            continue
        try:
            accumulator.counts.lines += 1
            log = convert.json2value(convert.utf82unicode(line))

            # FIX log.test TO BE A STRING
            if isinstance(log.test, list):
                log.test = " ".join(log.test)

            accumulator.__getattribute__(log.action)(log)
        except Exception, e:
            accumulator.counts.bad_lines += 1
            Log.warning("Problem with line\n{{line|indent}}", {"line": line}, e)

    output = accumulator.summary()
    Log.note("{{num_lines}} lines and {{num_tests}} tests in {{name}}", {"num_lines": output.counts.lines, "num_tests": output.counts.total, "name": name})
    return output


class LogSummary(object):
    def __init__(self):
        object.__setattr__(self, "tests", {})
        object.__setattr__(self, "attr", Struct())

    def __getattr__(self, item):
        return object.__getattribute__(self, "attr")[item]

    def __setattr__(self, key, value):
        object.__getattribute__(self, "attr")[key] = value

    def suite_start(self, log):
        pass

    def test_start(self, log):
        if isinstance(log.test, list):
            log.test = " ".join(log.test)
        self.tests[log.test] = Struct(
            test=log.test,
            start=log.time
        )

    def test_status(self, log):
        test = self.tests.get(log.test, None)
        if not test:
            self.tests[log.test] = test = Struct(
                test=log.test,
                start=log.time,
                missing_test_start=True
            )
        test.last_log = log.time
        test.stati[log.status.lower()] += 1

    def process_output(self, log):
        pass

    def log(self, log):
        if not log.test:
            return

        test = self.tests.get(log.test, None)
        if not test:
            self.tests[log.test] = test = Struct(
                test=log.test,
                start=log.time,
                missing_test_start=True
            )
        test.last_log = log.time
        test.counts.log_lines += 1

    def crash(self, log):
        if not log.test:
            return

        test = self.tests.get(log.test, None)
        if not test:
            self.tests[log.test] = test = Struct(
                test=log.test,
                start=log.time,
                crash=True,
                missing_test_start=True
            )
        test.last_log = log.time

    def test_end(self, log):
        test = self.tests.get(log.test, None)
        if not test:
            self.tests[log.test] = test = Struct(
                test=log.test,
                start=log.time,
                missing_test_start=True
            )

        test.ok = not log.expected
        test.result = log.status
        test.expected = nvl(log.expected, log.status)
        test.end = log.time
        test.duration = nvl(test.end - test.start, log.extra.runtime)
        test.extra = test.extra

        if not test.ok:
            Log.note("Bad test {{result}}", {"result": test})


    def suite_end(self, log):
        pass

    def summary(self):
        tests = wrap(self.tests.values())

        for t in tests:
            if not t.result:
                t.result = "NONE"
                t.end = t.last_log
                t.duration = t.end - t.start
                t.missing_test_end = True

        output = self.attr
        output.tests = tests
        output.counts.total = len(tests)
        output.counts.ok = len([t for t in tests if t.ok])
        # COUNT THE NUMBER OF EACH RESULT
        try:
            for r in set(tests.select("result")):
                output.counts[r.lower()] = len([t for t in tests if t.result == r])
        except Exception, e:
            Log.error("problem", e)

        return output


def transform_buildbot(payload):
    output = Q.select_one(payload, [
        {"name": "run.files", "value": "blobber_files"},
        {"name": "build.date", "value": "builddate"},
        {"name": "build.name", "value": "buildername"},
        {"name": "build.id", "value": "buildid"},
        {"name": "build.type", "value": "buildtype"},
        {"name": "build.url", "value": "buildurl"},
        {"name": "build.insertion_time", "value": "insertion_time"},
        {"name": "run.job_number", "value": "job_number"},
        {"name": "build.key", "value": "key"},
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

    output.run.timestamp = Date(output.run.timestamp).milli

    return output


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            es = Cluster(settings.destination).get_or_create_index(settings.destination)
            pulse = Pulse(settings.source, target=functools.partial(process_unittest, sink=es))

            Thread.wait_for_shutdown_signal()
            pulse.stop()
            pulse.join()

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
