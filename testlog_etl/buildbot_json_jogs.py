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
        "builder_id": 465665,
        "buildnumber": 0,
        "endtime": 1433209040,
        "id": 67106087,
        "master_id": 187,
        "properties": {
            "basedir": "/builds/slave/rel-m-beta-xr_sums-00000000000",
            "branch": "release-mozilla-beta",
            "build_number": 1,
            "buildername": "release-mozilla-beta-xulrunner_checksums",
            "buildnumber": 0,
            "log_url": "http://stage.mozilla.org/pub/mozilla.org/firefox/nightly/39.0b2-candidates/build1/logs/release-mozilla-beta-xulrunner_checksums-bm91-build1-build0.txt.gz",
            "master": "http://buildbot-master91.bb.releng.usw2.mozilla.com:8001/",
            "product": "Firefox",
            "project": "",
            "release_config": "mozilla/release-firefox-mozilla-beta.py",
            "repository": "",
            "request_ids": [71191057],
            "request_times": {"71191057": 1433208995},
            "scheduler": "release-mozilla-beta-xulrunner_deliverables_ready",
            "script_repo_revision": "51d8c8053b93",
            "script_repo_url": "https://hg.mozilla.org/build/tools",
            "slavebuilddir": "rel-m-beta-xr_sums-00000000000",
            "slavename": "bld-linux64-spot-316",
            "toolsdir": "/builds/slave/rel-m-beta-xr_sums-00000000000/scripts",
            "version": "39.0b2"
        },
        "reason": "downstream",
        "request_ids": [71191057],
        "requesttime": 1433208995,
        "result": 0,
        "slave_id": 6869,
        "starttime": 1433209007
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
    for i, p in enumerate(paths[100::]):  # FIRST ONE IS TODAY, AND INCOMPLETE, SO SKIP IT
        try:
            if i % 6 != 4:
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

            file = File("./results/" + p[:-3])
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
        except Exception, e:
            Log.warning("parse crash", cause=e)


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
