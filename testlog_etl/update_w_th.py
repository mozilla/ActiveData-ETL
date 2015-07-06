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
from pyLibrary.queries.qb_usingES import FromES
from testlog_etl.imports.treeherder import TreeHerder


def main():

    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        th = TreeHerder(settings=settings.hg)

        with FromES(settings=settings.elasticsearch) as es:
            some_failures = es.query({
                "from": "unittest",
                "where": {"and": [
                    {"eq": {"result.ok": False}},
                    # {"gt": {"run.timestamp": Date.today() - WEEK}},
                    {"missing": "treeherder.job.note"}
                    # {"eq": {
                    #     "build.branch": "mozilla-inbound",
                    #     "build.revision12": "7380457b8ba0"
                    # }}
                ]},
                "format": "list",
                "limit": 100
            })


            # th.get_markup("mozilla-inbound", "7380457b8ba0")
            for f in some_failures.data:
                mark = elasticsearch.scrub(th.get_markup(f))

                if mark:
                    es.update({
                        "set": {"treeherder": {"doc": mark}},
                        "where": {"eq": {"_id": f._id}}
                    })



    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


