
# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap
from pyLibrary.env import http
from pyLibrary.env.git import get_git_revision
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer
from testlog_etl import etl2key
from testlog_etl.transforms.pulse_block_to_job_logs import verify_equal, process_buildbot_log
from testlog_etl.transforms.pulse_block_to_unittest_logs import EtlHeadGenerator

_ = convert
DEBUG = True


def process(source_key, source, dest_bucket, resources, please_stop=None):
    etl_head_gen = EtlHeadGenerator(source_key)
    counter = 0
    output = []

    for i, buildbot_line in enumerate(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        data = convert.json2value(buildbot_line)
        if not data:
            continue

        url = data.run.logurl

        data.etl = wrap({
            "id": counter,
            "file": url,
            "timestamp": Date.now().unix,
            "revision": get_git_revision(),
            "source": {
                "id": 0,
                "source": data.etl,
                "type": "join"
            },
            "type": "join"
        })

        with Timer("Read {{url}}", {"url": url}, debug=DEBUG) as timer:
            try:
                if url == None:
                    data.etl.error = "No logurl"
                    output.append(data)
                    continue
                response = http.get(
                    url=url,
                    retry={"times": 3, "sleep": 10}
                )
                if response.status_code == 404:
                    data.etl.error = "Text log unreachable"
                    output.append(data)
                    continue

                all_log_lines = response._all_lines(encoding='latin1')
                data.action = process_buildbot_log(all_log_lines, url)

                verify_equal(data, "build.revision", "action.revision")
                verify_equal(data, "build.id", "action.buildid")
                verify_equal(data, "run.machine.name", "action.slave")

                output.append(data)
                Log.note("Found builder record for id={{id}}", id=etl2key(data.etl))
            except Exception, e:
                Log.warning("Problem processing {{url}}", url=url, cause=e)
                data.etl.error = "Text log unreachable"
                output.append(data)
            finally:
                counter += 1
                etl_head_gen.next_id = 0

        data.etl.duration = timer.duration

    dest_bucket.extend([{"id": etl2key(d.etl), "value": d} for d in output])
    return {source_key + ".0"}

