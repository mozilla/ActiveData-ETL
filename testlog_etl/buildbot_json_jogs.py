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

from pyLibrary import convert, strings
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.env.files import File
from pyLibrary.jsons import stream
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.thread.threads import Queue, Thread
from testlog_etl.imports.buildbot import BuildbotTranslator


ACTIVE_DATA = "http://activedata.allizom.org/query"

def compare_to_es(settings):
    url = "http://builddata.pub.build.mozilla.org/builddata/buildjson/"


    b = BuildbotTranslator()
    b.parse({
        "builder_id": 409530,
        "buildnumber": 0,
        "endtime": 1437072629,
        "id": 70184225,
        "master_id": 183,
        "properties": {
            "basedir": "C:\\slave\\test",
            "branch": "addon-sdk",
            "buildername": "jetpack-fx-team-win7-ix-opt",
            "buildnumber": 0,
            "commit_titles": [
                "Merge pull request #1448 from ZER0/panel-click/858976",
                "Bug 858976 - cmd+click a link in a panel should open the link in a new tab"
            ],
            "log_url": "http://ftp.mozilla.org/pub/mozilla.org/jetpack/tinderbox-builds/addon-sdk-win7-ix/jetpack-fx-team-win7-ix-opt-bm111-tests1-windows-build0.txt.gz",
            "master": "http://buildbot-master111.bb.releng.scl3.mozilla.com:8201/",
            "platform": "win7-ix",
            "product": "jetpack",
            "project": "",
            "repository": "",
            "request_ids": [75011601],
            "request_times": {"75011601": 1437071679},
            "revision": "42d1d4d63c204329d1e293d1fba5521f17379afa",
            "scheduler": "jetpack",
            "script_repo_revision": "da436987c292",
            "script_repo_url": "https://hg.mozilla.org/build/tools",
            "slavename": "t-w732-ix-051"
        },
        "reason": "scheduler",
        "request_ids": [75011601],
        "requesttime": 1437071679,
        "result": 1,
        "slave_id": 4576,
        "starttime": 1437071939
    }


    )



    # GET LIST OF LOGS
    paths = []
    response = http.get(url)
    for line in response.all_lines:
        # <tr><td valign="top"><img src="/icons/compressed.gif" alt="[   ]"></td><td><a href="builds-2015-09-20.js.gz">builds-2015-09-20.js.gz</a></td><td align="right">20-Sep-2015 19:00  </td><td align="right">6.9M</td><td>&nbsp;</td></tr>
        filename = strings.between(line, '</td><td><a href=\"', '">')
        if filename and filename.startswith("builds-2"):  # ONLY INTERESTED IN DAILY SUMMARY FILES (eg builds-2015-09-20.js.gz)
            paths.append(filename)
        paths = qb.reverse(qb.sort(paths))

    # paths = [None, "builds-2015-08-29.js.gz", "builds-2015-08-17.js.gz"]
    for i, p in enumerate(paths[120::]):  # FIRST ONE IS TODAY, AND INCOMPLETE, SO SKIP IT
        if i % 6 != 1:
            continue
        full_path = url + p
        Log.note("process {{url}}", url=full_path)
        response = http.get(full_path)
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        def json():
            last_bytes_count = 0  # Track the last byte count, so we do not show too many
            bytes_count = 0
            while True:
                bytes_ = response.raw.read(4096)
                if not bytes_:
                    return
                data = decompressor.decompress(bytes_)
                bytes_count += len(data)
                if Math.floor(last_bytes_count, 1000000) != Math.floor(bytes_count, 1000000):
                    last_bytes_count = bytes_count
                    Log.note("bytes={{bytes}}", bytes=bytes_count)
                yield data

        tasks = stream.parse(
            json(),
            "builds",
            expected_vars=["builds"]
        )

        # file = File("./tests/resources/buildbot_example.json")
        # file.delete()
        # temp = []
        # for i, ts in qb.groupby(tasks, size=1000):
        #     # file.extend(map(convert.value2json, ts.builds))
        #     temp.extend(ts.builds)
        #     Log.note("done {{i}}", i=i)
        # tasks = wrap(temp)

        # tasks = File("./tests/resources/buildbot_example.json").read_json()

        result = Queue("")

        file = File("./results/" + p)
        file.delete()
        def writer(please_stop):
            while True:
                j = result.pop_all()
                if j:
                    file.extend(map(convert.value2json, j))
                else:
                    Thread.sleep(1)
        Thread.run("writer", writer)

        b = BuildbotTranslator()
        for t in tasks:
            try:
                result.add(b.parse(t['builds']))
            except Exception, e:
                Log.warning("problem in {{path}}", path=full_path, cause=e)
        result.add(Thread.STOP)
        # Log.note("Number of builds = {{count}}", count=len(tasks))
        # es = elasticsearch.Index(settings.elasticsearch)
        #
        # # FIND IN ES
        # found = http.get_json(url=ACTIVE_DATA, json={
        #     "from": "jobs",
        #     "select": ["_id", "run.key", "run.logurl", "action.start_time", "action.end_time"],
        #     "where": {"and": [
        #         {"gte": {"action.start_time": Math.floor(Math.MIN(tasks.starttime), DAY.seconds) - DAY.seconds}},
        #         {"lt": {"action.start_time": Math.ceiling(Math.MAX(tasks.endtime), DAY.seconds) + DAY.seconds}}
        #     ]},
        #     "limit": 1000,
        #     "format": "list"
        # })
        #
        # existing = Index(keys="run.logurl", data=found)
        #
        # count=0
        # for t in tasks:
        #     if any(map(t.properties.slavename.startswith, ["b-2008", "bld-linux", "bld-lion"])):
        #         continue
        #     e = existing[t.properties.log_url]
        #     if not e:
        #         count+=1
        #         Log.note("missing\n{{task}}", task=t)
        #
        # Log.note("missing count = {{count}}", count=count)



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
