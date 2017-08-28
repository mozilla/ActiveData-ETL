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

from future.utils import text_type
from mo_json import json2value, value2json
from mo_logs import Log, strings
from mo_dots import wrap, Data, literal_field
from pyLibrary.env import http
from pyLibrary.env.git import get_git_revision
from mo_times.dates import Date
from mo_times.timer import Timer

DEBUG = False
DEBUG_SHOW_LINE = True
DEBUG_SHOW_NO_LOG = False
TOO_MANY_FAILS = 5  # STOP LOOKING AT AN ARTIFACT AFTER THIS MANY WITH NON-JSON LINES

TRY_AGAIN_LATER = "{{reason}}, try again later"

STRUCTURED_LOG_ENDINGS = [
    "structured_logs.log",
    "_structured_full.log",
    '_raw.log',
    '.jsonl'
]
NOT_STRUCTURED_LOGS = [
    ".apk",
    "/awsy_raw.log",
    "/buildbot_properties.json",
    "/buildprops.json",
    "/chain_of_trust.log"
    "/chainOfTrust.json.asc",
    "/talos_raw.log",
    ".mozinfo.json",
    "_errorsummary.log",
    ".exe",
    "/log_critical.log",
    "/log_error.log",
    "/log_fatal.log",
    "/log_info.log",
    "/log_warning.log",
    "/manifest.json",
    "/mar.exe",
    "/mbsdiff.exe",
    "/mozharness.zip",
    "/properties.json",
    "/log_raw.log",
    "/localconfig.json",
    "/talos_critical.log",
    "/talos_error.log",
    "/talos_fatal.log",
    "/talos_info.log",
    "/talos_warning.log",
    "/live.log",
    "/live_backing.log",
    "/master.tar.gz",
    ".tests.zip",
    ".checksums.asc",
    ".checksums",
    ".langpack.xpi",
    "/.tar.gz",
    ".test_packages.json",
    "/xvfb.log",
    "/xsession-errors.log",
    "/resource-usage.json",
    ".html",
    ".pom.sha1",
    ".pom",
    ".xml.sha1",
    ".xml",
    ]
TOO_MANY_NON_JSON_LINES = Data()

next_key = {}  # TRACK THE NEXT KEY FOR EACH SOURCE KEY


class Transform(object):

    def __call__(self, source_key, source, destination, resources, please_stop=None):
        """
        :param source_key: THE DOT-DELIMITED PATH FOR THE SOURCE
        :param source: A LINE GENERATOR WITH ETL ARTIFACTS (LIKELY JSON)
        :param destination: THE s3 BUCK TO PLACE ALL THE TRANSFORM RESULTS
        :param resources: VARIOUS EXTRA RESOURCES TO HELP WITH ANNOTATING THE DATA
        :param please_stop: CHECK REGULARITY, AND EXIT TRANSFORMATION IF True
        :return: list OF NEW KEYS, WITH source_key AS THEIR PREFIX
        """
        raise NotImplementedError


def verify_blobber_file(line_number, name, url):
    """
    :param line_number:  for debugging
    :param name:  for debugging
    :param url:  TO BE READ
    :return:  RETURNS BYTES **NOT** UNICODE
    """
    if any(map(name.endswith, NOT_STRUCTURED_LOGS)):
        return None, 0
    if name.find("/jscov_") >= 0 and name.endswith(".json"):
        return None, 0

    with Timer("Read {{name}}: {{url}}", {"name": name, "url": url}, debug=DEBUG):
        response = http.get(url)

        try:
            logs = response.all_lines
        except Exception as e:
            if name.endswith("_raw.log"):
                Log.error(
                    "Line {{line}}: {{name}} = {{url}} is NOT structured log",
                    line=line_number,
                    name=name,
                    url=url,
                    cause=e
                )
            if DEBUG:
                Log.note(
                    "Line {{line}}: {{name}} = {{url}} is NOT structured log",
                    line=line_number,
                    name=name,
                    url=url
                )
            return None, 0

    if any(name.endswith(e) for e in STRUCTURED_LOG_ENDINGS):
        # FAST TRACK THE FILES WE SUSPECT TO BE STRUCTURED LOGS ALREADY
        return logs, "unknown"

    if TOO_MANY_NON_JSON_LINES[literal_field(name)] >= TOO_MANY_FAILS:
        return None, 0

    # DETECT IF THIS IS A STRUCTURED LOG

    with Timer("Structured log detection {{name}}:", {"name": name}):
        try:
            total = 0  # ENSURE WE HAVE A SIDE EFFECT
            count = 0
            bad = 0
            for blobber_line in logs:
                blobber_line = strings.strip(blobber_line)
                if not blobber_line:
                    continue

                try:
                    total += len(json2value(blobber_line))
                    count += 1
                except Exception as e:
                    if DEBUG:
                        Log.note("Not JSON: {{line}}",
                            name= name,
                            line= blobber_line)
                    bad += 1
                    if bad > 4:
                        TOO_MANY_NON_JSON_LINES[literal_field(name)] += 1
                        Log.error("Too many bad lines")

            if count == 0:
                # THERE SHOULD BE SOME JSON TO BE A STRUCTURED LOG
                TOO_MANY_NON_JSON_LINES[literal_field(name)] += 1
                Log.error("No JSON lines found")

        except Exception as e:
            if name.endswith("_raw.log") and "No JSON lines found" not in e:
                Log.error(
                    "Line {{line}}: {{name}} is NOT structured log",
                    line=line_number,
                    name=name,
                    cause=e
                )
            if DEBUG:
                Log.note(
                    "Line {{line}}: {{name}} is NOT structured log",
                    line=line_number,
                    name=name
                )
            return None, 0

    return logs, count


class EtlHeadGenerator(object):
    """
    WILL RETURN A UNIQUE ETL STRUCTURE, GIVEN A SOURCE AND A DESTINATION NAME
    """

    def __init__(self, source_key):
        self.source_key = source_key
        self.next_id = 0

    def next(
        self,
        source_etl,  # ETL STRUCTURE DESCRIBING SOURCE
        name=None,  # NAME FOR HUMANS TO BETTER UNDERSTAND WHICH SOURCE THIS IS
        url=None  # URL FOR THE DATA
    ):
        num = self.next_id
        self.next_id = num + 1
        dest_key = self.source_key + "." + text_type(num)

        dest_etl = wrap({
            "id": num,
            "name": name,
            "url": url,
            "source": source_etl,
            "type": "join",
            "revision": get_git_revision(),
            "timestamp": Date.now().unix
        })

        return dest_key, dest_etl
