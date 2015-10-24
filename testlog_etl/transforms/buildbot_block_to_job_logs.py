
# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
import zlib

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, Dict, set_default
from pyLibrary.env import http
from pyLibrary.env.big_data import GzipLines, ZipfileLines, ibytes2ilines
from pyLibrary.env.git import get_git_revision
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key
from testlog_etl.imports.buildbot import BuildbotTranslator
from testlog_etl.transforms.pulse_block_to_job_logs import verify_equal, process_buildbot_log
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator

_ = convert
DEBUG = True


def process(source_key, source, dest_bucket, resources, please_stop=None):
    bb = BuildbotTranslator()

    output = []

    for buildbot_line in source.read_lines():
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        buildbot_data = convert.json2value(buildbot_line)
        try:
            data = bb.parse(buildbot_data.builds)
            rev = Dict(
                changeset={"id": data.build.revision},
                branch={"name": data.build.branch, "locale": data.build.locale}
            )
            data.repo = resources.hg.get_revision(rev)
        except Exception, e:
            Log.warning(
                "Can not get revision for branch={{branch}}, locale={{locale}}, revision={{revision}}\n{{details|json|indent}}",
                branch=data.build.branch,
                locale=data.build.locale,
                revision=data.build.revision,
                details=data,
                cause=e
            )

        url = data.run.logurl
        data.etl = set_default(
            {
                "file": url,
                "timestamp": Date.now().unix,
                "revision": get_git_revision(),
            },
            buildbot_data.etl
        )

        with Timer("Read {{url}}", {"url": url}, debug=DEBUG) as timer:
            try:
                if url == None:
                    data.etl.error = "No logurl"
                    output.append(data)
                    continue

                if "scl3.mozilla.com" in url:
                    Log.alert("Will not read {{url}}", url=url)
                    data.etl.error = "Text log unreachable"
                    output.append(data)
                    continue

                response = http.get(
                    url=[
                        url,
                        url.replace("http://ftp.mozilla.org", "http://archive.mozilla.org"),
                        url.replace("http://ftp.mozilla.org", "http://ftp-origin-scl3.mozilla.org")
                    ],
                    retry={"times": 3, "sleep": 10}
                )
                if response.status_code == 404:
                    data.etl.error = "Text log unreachable"
                    output.append(data)
                    continue

                if url.endswith(".gz"):
                    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
                    def ibytes():
                        while True:
                            bytes_ = response.raw.read(4096)
                            if not bytes_:
                                return
                            data = decompressor.decompress(bytes_)
                            yield data
                    lines = ibytes2ilines(ibytes())
                else:
                    lines = response._all_lines(encoding='latin1')
                all_log_lines = lines
                action = process_buildbot_log(all_log_lines, url)
                set_default(data.action, action)
                data.action.duration = data.action.end_time - data.action.start_time

                verify_equal(data, "build.revision", "action.revision")
                verify_equal(data, "build.id", "action.buildid")
                verify_equal(data, "run.key", "action.builder", warning=False)
                verify_equal(data, "run.machine.name", "action.slave")

                output.append(data)
                Log.note("Found builder record for id={{id}}", id=etl2key(data.etl))
            except Exception, e:
                Log.warning("Problem processing {{url}}", url=url, cause=e)
                data.etl.error = "Text log unreachable"
                output.append(data)

        data.etl.duration = timer.duration

    dest_bucket.extend([{"id": etl2key(d.etl), "value": d} for d in output])
    return {source_key}

