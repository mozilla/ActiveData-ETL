
# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from activedata_etl.imports.resource_usage import normalize_resource_usage
from activedata_etl.transforms import TRY_AGAIN_LATER
from pyLibrary import convert
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, set_default
from pyLibrary.env import elasticsearch, http
from pyLibrary.env.git import get_git_revision
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import MONTH
from pyLibrary.times.timer import Timer
from activedata_etl import etl2key
from activedata_etl.imports.buildbot import BuildbotTranslator
from activedata_etl.transforms.pulse_block_to_job_logs import verify_equal, process_buildbot_log

_ = convert
DEBUG = False
TOO_OLD = (Date.today()-MONTH).unix


def process(source_key, source, dest_bucket, resources, please_stop=None):
    bb = BuildbotTranslator()
    output = []

    for buildbot_line in list(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        buildbot_data = convert.json2value(buildbot_line)
        try:
            data = bb.parse(buildbot_data.builds)
        except Exception, e:
            Log.error(
                "Can not parse\n{{details|json|indent}}",
                details=buildbot_data,
                cause=e
            )

        # RESOURCE USAGE
        try:
            for a in data.run.files:
                if a.name == "resource-usage.json":
                    content = http.get_json(a.url)
                    data.resource_usage = normalize_resource_usage(content)
                    break
        except Exception, e:
            Log.warning("Could not process resource-usage.json", cause=e)

        #TREEHERDER MARKUP
        try:
            if data.build.revision:
                data.treeherder = resources.treeherder.get_markup(
                    data.build.branch,
                    data.build.revision,
                    None,
                    data.run.key,
                    data.action.end_time
                )
        except Exception, e:
            if TRY_AGAIN_LATER in e:
                Log.error("Aborting processing of {{key}}", key=source_key, cause=e)

            Log.warning(
                "Could not lookup Treeherder data for {{key}} and revision={{revision}}",
                key=source_key,
                revision=data.build.revision12,
                cause=e
            )

        if data.action.start_time < TOO_OLD:
            Log.warning("Do not try to process old buildbot logs")
            return set()

        try:
            rev = Dict(
                changeset={"id": data.build.revision},
                branch={"name": data.build.branch, "locale": data.build.locale}
            )
            data.repo = resources.hg.get_revision(rev)
        except Exception, e:
            if data.action.start_time > Date.today()-MONTH:
                # ONLY SEND WARNING IF IT IS RECENT
                send = Log.warning
            else:
                send = Log.note

            send(
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
                    Log.note("Text log does not exist {{url}}", url=url)
                    data.etl.error = "Text log does not exist"
                    output.append(data)
                    continue

                all_log_lines = response._all_lines(encoding=None)
                action = process_buildbot_log(all_log_lines, url)
                set_default(data.action, action)
                data.action.duration = data.action.end_time - data.action.start_time

                verify_equal(data, "build.revision", "action.revision", url, from_key=source_key)
                verify_equal(data, "build.id", "action.buildid", url, from_key=source_key)
                verify_equal(data, "run.key", "action.builder", warning=False, from_url=url, from_key=source_key)
                verify_equal(data, "run.machine.name", "action.slave", from_url=url, from_key=source_key)

                output.append(elasticsearch.scrub(data))
                Log.note("Found builder record for id={{id}}", id=etl2key(data.etl))
            except Exception, e:
                e = Except.wrap(e)  # SO `in` OPERATOR WORKS
                if "Problem with calculating durations" in e:
                    Log.error("Prioritized error", cause=e)
                elif "Connection reset by peer" in e:
                    Log.error("Connectivity problem", cause=e)
                elif "incorrect header check" in e:
                    Log.error("problem reading", cause=e)

                Log.warning("Problem processing {{key}}: {{url}}", key=source_key, url=url, cause=e)
                data.etl.error = "Text log unreadable"
                output.append(data)

        data.etl.duration = timer.duration

    dest_bucket.extend([{"id": etl2key(d.etl), "value": d} for d in output])
    return {source_key}

