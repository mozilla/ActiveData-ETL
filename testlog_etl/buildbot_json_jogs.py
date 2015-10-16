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
import zlib

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, set_default
from pyLibrary.env import http
from pyLibrary.jsons import stream
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.queries.index import Index
from pyLibrary.testing import elasticsearch
from pyLibrary.times.durations import SECOND, DAY


ACTIVE_DATA = "http://activedata.allizom.org/query"

def compare_to_es(settings):
    url = "http://builddata.pub.build.mozilla.org/builddata/buildjson/"

    # GET LIST OF LOGS
    # paths = []
    # response = http.get(url)
    # for line in response.all_lines:
    #     # <tr><td valign="top"><img src="/icons/compressed.gif" alt="[   ]"></td><td><a href="builds-2015-09-20.js.gz">builds-2015-09-20.js.gz</a></td><td align="right">20-Sep-2015 19:00  </td><td align="right">6.9M</td><td>&nbsp;</td></tr>
    #     filename = strings.between(line, '</td><td><a href=\"', '">')
    #     if filename and filename.startswith("builds-2"):  # ONLY INTERESTED IN DAILY SUMMARY
    #         paths.append(filename)
    #     paths = qb.reverse(qb.sort(paths))

    paths = ['', 'builds-2015-10-13.js.gz']

    for p in paths[1:]:  # FIRST ONE IS TODAY, AND INCOMPLETE, SO SKIP IT
        response = http.get(url + p)
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        def json():
            total_bytes = 0
            while True:
                bytes_ = response.raw.read(4096)
                if not bytes_:
                    return
                data = decompressor.decompress(bytes_)
                total_bytes += len(data)
                Log.note("bytes={{bytes}}", bytes=total_bytes)
                yield data

        tasks = stream.parse(
            json(),
            "builds",
            expected_vars=[
                "builds.starttime",
                "builds.endtime",
                "builds.requesttime",
                "builds.reason",
                "builds.properties.request_times",
                "builds.properties.slavename",
                "builds.properties.log_url",
                "builds.properties.buildername"
            ]
        )

        temp = []
        for i, t in enumerate(tasks):
            temp.append(t['builds'])
            if i > 200:
                break
        tasks = wrap(temp)

        Log.note("Number of builds = {{count}}", count=len(tasks))
        es = elasticsearch.Index(settings.elasticsearch)

        # FIND IN ES
        found = http.get_json(url=ACTIVE_DATA, json={
            "from": "jobs",
            "select": ["_id", "run.key", "run.logurl", "action.start_time", "action.end_time"],
            "where": {"and": [
                {"gte": {"action.start_time": Math.floor(Math.MIN(tasks.starttime), DAY.seconds) - DAY.seconds}},
                {"lt": {"action.start_time": Math.ceiling(Math.MAX(tasks.endtime), DAY.seconds) + DAY.seconds}}
            ]},
            "limit": 1000,
            "format": "list"
        })

        existing = Index(keys="run.logurl", data=found)

        count=0
        for t in tasks:
            if any(map(t.properties.slavename.startswith, ["b-2008", "bld-linux", "bld-lion"])):
                continue
            e = existing[t.properties.log_url]
            if not e:
                count+=1
                Log.note("missing\n{{task}}", task=t)

        Log.note("missing count = {{count}}", count=count)



def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        compare_to_es(settings)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
