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
        "builder_id": 302291,
        "buildnumber": 103,
        "endtime": 1423666999,
        "id": 58810849,
        "master_id": 124,
        "properties": {
            "appName": "Firefox",
            "appVersion": "38.0a1",
            "aws_ami_id": "ami-f6f8b09e",
            "aws_instance_id": "i-80b91e7a",
            "aws_instance_type": "r3.xlarge",
            "basedir": "/builds/slave/m-cen-l64-mulet-00000000000000",
            "branch": "mozilla-central",
            "builddir": "m-cen-l64-mulet-00000000000000",
            "buildername": "Linux x86-64 Mulet mozilla-central build",
            "buildnumber": 103,
            "builduid": "72e10b44feb6443fbe264765c13dc8e6",
            "comments": "merge mozilla-inbound to mozilla-central a=merge",
            "commit_titles": [
                "merge mozilla-inbound to mozilla-central a=merge",
                "Bug 1131700 - ServiceWorkerManager::CreateServiceWorker should use",
                "Bug 1130932 - allow GMPDecryptorParent::RecvKeyStatusChanged calls after Close(). r=edwin.",
                "Bug 1130917 - Part 3 - fix EME gtests. r=edwin.",
                "Bug 1130917 - Part 2 - improve error handling of StoreData() and ReadData(). r=edwin.",
                "Bug 1130917 - Part 1 - disallow multiple records with the same name in",
                "Bug 1130256 - Prevent unwanted scrolling event. r=roc",
                "Bug 1129173 - Properly detect B2G for workers interface tests. r=bent",
                "Bug 1129148 - Wrote a MACRO to generate conditions to inline SIMD instructions (sub, mul, and, or,",
                "Bug 1121722 - Chrome-only DOM File constructors should use lastModified attribute. r=bz",
                "Bug 1130754: Avoid recalculating tbsCertificate digest, r=keeler"
            ],
            "forced_clobber": False,
            "got_revision": "38058cb42a0e",
            "hashType": "sha512",
            "jsshellUrl": "http://ftp.mozilla.org/pub/mozilla.org/b2g/tinderbox-builds/mozilla-central-linux64-mulet/1423663138/jsshell-linux-x86_64.zip",
            "log_url": "http://ftp.mozilla.org/pub/mozilla.org/b2g/tinderbox-builds/mozilla-central-linux64-mulet/1423663138/mozilla-central-linux64-mulet-bm71-build1-build103.txt.gz",
            "master": "http://buildbot-master71.srv.releng.use1.mozilla.com:8001/",
            "packageFilename": "firefox-38.0a1.en-US.linux-x86_64.tar.bz2",
            "packageHash": "3bd8624c6339882d9a5bff7b169d34467fb9dcd27aaf0aba540c8576bc1c43f16a96ba0e9dda6e270365f1fefcd179f475f495263fd79002a961b61ddb078439",
            "packageSize": "58164963",
            "packageUrl": "http://ftp.mozilla.org/pub/mozilla.org/b2g/tinderbox-builds/mozilla-central-linux64-mulet/1423663138/firefox-38.0a1.en-US.linux-x86_64.tar.bz2",
            "periodic_clobber": False,
            "placement/availability_zone": "us-east-1d",
            "platform": "linux64-mulet",
            "product": "b2g",
            "project": "",
            "purge_actual": "58.94GB",
            "purge_target": "15GB",
            "purged_clobber": True,
            "repository": "",
            "request_ids": [61696936],
            "request_times": {"61696936": 1423663139},
            "revision": "38058cb42a0ee28016d2cc619568b45249202799",
            "scheduler": "b2g_mozilla-central-b2g",
            "slavebuilddir": "m-cen-l64-mulet-00000000000000",
            "slavename": "bld-linux64-spot-045",
            "stage_platform": "linux64-mulet",
            "symbolsUrl": "http://ftp.mozilla.org/pub/mozilla.org/b2g/tinderbox-builds/mozilla-central-linux64-mulet/1423663138/firefox-38.0a1.en-US.linux-x86_64.crashreporter-symbols.zip",
            "testsUrl": "http://ftp.mozilla.org/pub/mozilla.org/b2g/tinderbox-builds/mozilla-central-linux64-mulet/1423663138/firefox-38.0a1.en-US.linux-x86_64.tests.zip",
            "toolsdir": "/builds/slave/m-cen-l64-mulet-00000000000000/tools"
        },
        "reason": "scheduler",
        "request_ids": [61696936],
        "requesttime": 1423663139,
        "result": 0,
        "slave_id": 6952,
        "starttime": 1423663669
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

    paths = [None, "builds-2015-02-11.js.gz"]
    for i, p in enumerate(paths[1::]):  # FIRST ONE IS TODAY, AND INCOMPLETE, SO SKIP IT
        try:
            # if i % 6 != 1:
            #     continue
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
