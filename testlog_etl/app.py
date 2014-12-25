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



def process(settings, queue, please_stop):
    es = Cluster(settings.destination).get_or_create_index(settings.destination)
    with Pulse(settings.source, queue=queue):
        for data in queue:
            meta = transform_buildbot(data)
            Log.note("{{data}}", {"data": meta})

            found_log = False
            for name, url in meta.run.files.items():
                if "structured" in name and name.endswith(".log"):
                    found_log = True
                    with Timer("Process log {{file}}", {"file": name}):
                        results = process_log(name, url)
                    meta.counts = results.counts
                    es.extend([{"value": set_default({"result": t}, meta)} for t in results.tests])

            if not found_log:
                Log.note("NO STRUCTURED LOG")


def process_log(name, url):
    accumulator = LogSummary(name)
    accumulator.suite_start()

    response = requests.get(url, stream=True)
    num_lines = 0
    for line in response.iter_lines():
        num_lines += 1
        try:
            if "test_start" in line:
                log = convert.json2value(convert.utf82unicode(line))
                if log.action == "test_start":
                    accumulator.test_start(log)
                    continue

            if "test_end" in line:
                log = convert.json2value(convert.utf82unicode(line))
                if log.action == "test_end":
                    accumulator.test_end(log)
                    continue
        except Exception, e:
            Log.warning("Problem with line\n{{line|indent}}", {"line": line}, e)

    output = accumulator.suite_end()
    output.counts.lines = num_lines
    Log.note("{{num_lines}} lines and {{num_tests}} tests in {{name}}", {"num_lines": num_lines, "num_tests":  output.counts.total, "name": name})
    return output


class LogSummary(object):
    def __init__(self, url):
        self.url = url
        self.tests = {}


    def suite_start(self):
        pass

    def test_start(self, log):
        if isinstance(log.test, list):
            log.test = " ".join(log.test)
        self.tests[log.test] = Struct(
            test=log.test,
            start=log.time
        )

    def test_end(self, log):
        if isinstance(log.test, list):
            log.test = " ".join(log.test)
        if log.test in self.tests:
            test = self.tests[log.test]
            test.ok = not log.expected
            test.result = log.status
            test.expected = nvl(log.expected, log.status)
            test.end = log.time
            test.duration = test.end - test.start
            test.extra = test.extra
        else:
            self.tests[log.test] = test = Struct(
                ok=not log.expected,
                result=log.status,
                expected=nvl(log.expected, log.status),
                test=log.test,
                end=log.time,
                duration=log.extra.runtime,
                extra=log.extra,
                missing_test_start=True
            )
            if test.duration:
                test.start = test.end - test.duration

        if not test.ok:
            Log.note("Bad test {{result}}", {"result": test})

    def suite_end(self):
        tests = wrap(self.tests.values())
        output = Struct(
            url=self.url,
            tests=tests
        )
        output.counts.total = len(tests)
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
            queue = Queue()

            thread = Thread.run("processing", process, settings, queue)
            Thread.wait_for_shutdown_signal()
            queue.add(Thread.STOP)
            thread.stop()
            thread.join()

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
