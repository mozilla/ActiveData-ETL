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
from pyLibrary.structs import wrap, set_default, Struct
from pyLibrary.thread.threads import Thread


def got_result(es, data, message):
    meta = transform(wrap(data).payload)
    Log.note("{{data}}", {"data": meta})

    for name, url in meta.run.files.items():
        if "structured" in name and name.endswith(".log"):
            results = process_log(name, url)
            es.extend([{"value": set_default({"result": r}, meta)} for r in results])
        else:
            Log.note("NO STRUCTURED LOG")

    message.ack()


def process_log(name, url):
    response = requests.get(url, stream=True)
    num_lines = 0
    tests = {}
    for line in response.iter_lines():
        num_lines += 1

        if "test_start" in line:
            log = convert.json2value(convert.utf82unicode(line))
            if log.action == "test_start":
                tests[log.test] = Struct(
                    test=log.test,
                    start=log.time
                )

        if "test_end" in line:
            log = convert.json2value(convert.utf82unicode(line))
            if log.action == "test_end":
                test = tests[log.test]
                test.end = log.time
                test.duration = test.end - test.start
                test.runtime = test.extra.runtime
                test.ok = (log.expected == None)
                test.status = log.status
                test.expected = log.expected

                if not test.ok:
                    Log.note("Bad test {{result}}", {"result": test})

    output = tests.values()
    Log.note("{{num_lines}} lines and {{num_tests}} tests in {{name}}", {"num_lines": num_lines, "num_tests": len(output), "name": name})
    return output

def transform(payload):
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

    return output



def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            es = Cluster(settings.destination).get_or_create_index(settings.destination)
            with Pulse(settings.source, functools.partial(got_result, *(es, ))):
                Thread.sleep()

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
