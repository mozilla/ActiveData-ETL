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
from pyLibrary.env import elasticsearch
from pyLibrary.queries.jx_usingES import FromES
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY, MONTH
from testlog_etl.imports.treeherder import TreeHerder


def main():

    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        th = TreeHerder(settings=settings.hg, use_cache=True)

        with FromES(read_only=False, settings=settings.elasticsearch) as es:
            while True:
                some_failures = es.query({
                    "from": "unittest",
                    "where": {"and": [
                        {"eq": {"result.ok": False}},
                        {"or": [
                            {"missing": "treeherder.etl.timestamp"},
                            {"lt": {"treeherder.etl.timestamp": Date.today() - DAY}}
                        ]},
                        {"gt": {"run.timestamp": Date.today() - MONTH}}
                    ]},
                    "format": "list",
                    "limit": 10000
                })


                # th.get_markup("mozilla-inbound", "7380457b8ba0")
                updates = 0
                for f in some_failures.data:
                    mark = elasticsearch.scrub(th.get_markup(f))

                    if mark and mark.stars or mark.notes:
                        if f.treeherder != mark:
                            updates += 1
                            es.update({
                                "set": {"treeherder": {"doc": mark}},
                                "where": {"eq": {"_id": f._id}}
                            })
                Log.note("{{num}} updates sent to ES", num=updates)
                if updates == 0:
                    break

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


