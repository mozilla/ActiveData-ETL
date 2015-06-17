# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY, WEEK
from testlog_etl.imports.treeherder import TreeHerder


def main():

    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        some_failures = http.post_json("http://activedata.allizom.org/query", data={
            "from": "unittest",
            "select": [
                {"name": "branch", "value": "build.branch"},
                {"name": "revision", "value": "build.revision12"},
                {"name": "suite", "value": "run.suite"},
                {"name": "chunk", "value": "run.chunk"},
                {"name": "test", "value": "result.test"}
            ],
            "where": {"and": [
                {"eq": {"result.ok": False}},
                {"gt": {"run.timestamp": Date.today() - WEEK}}
            ]},
            "format": "list",
            "limit": 10
        })


        th = TreeHerder(settings={})

        th.get_job_classification("mozilla-inbound", "7380457b8ba0")
        # for f in some_failures.data:
        #     th.get_job_classification(f.branch, f.revision)

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


