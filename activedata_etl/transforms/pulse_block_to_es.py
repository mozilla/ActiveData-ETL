# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from activedata_etl import etl2key
from activedata_etl.imports.buildbot import BuildbotTranslator
from activedata_etl.transforms import TRY_AGAIN_LATER
from mo_dots import Data, Null
from mo_hg.hg_mozilla_org import DEFAULT_LOCALE, minimize_repo
from mo_hg.repos.changesets import Changeset
from mo_hg.repos.revisions import Revision
from mo_json import json2value
from mo_logs import Log, strings
from mo_threads.profiles import Profiler
from pyLibrary.env.git import get_git_revision

DEBUG = True
bb = BuildbotTranslator()


def process(source_key, source, destination, resources, please_stop=None):

    lines = source.read_lines()

    keys = []
    records = []
    stats = Data()
    for i, line in enumerate(lines):
        if please_stop:
            Log.error("Unexpected request to stop")
        pulse_record = Null
        try:
            pulse_record = scrub_pulse_record(source_key, i, line, stats)
            if not pulse_record:
                continue

            with Profiler("transform_buildbot"):
                record = transform_buildbot(source_key, pulse_record.payload, resources=resources)
                key = pulse_record._meta.routing_key
                key_path = key.split(".")
                pulse_id = ".".join(key_path[:-1])
                pulse_action = key_path[-1]

                record.etl = {
                    "id": i,
                    "source": pulse_record.etl,
                    "type": "join",
                    "revision": get_git_revision(),
                    "pulse_key": pulse_id,
                    "pulse_action": pulse_action
                }
            key = etl2key(record.etl)
            keys.append(key)
            records.append({"id": key, "value": record})
        except Exception as e:
            if TRY_AGAIN_LATER:
                Log.error("Did not finish processing {{key}}", key=source_key, cause=e)
            Log.warning("Problem with pulse payload {{pulse|json}}", pulse=pulse_record.payload, cause=e)
    destination.extend(records)
    return keys


def scrub_pulse_record(source_key, i, line, stats):
    """
    DUE TO MANY ETL FORMATS, THIS IS REQUIRED TO
    TURN RAW LINE INTO A STANDARD PULSE RECORD
    """
    try:
        line = strings.strip(line)
        if not line:
            return None
        pulse_record = json2value(line)
        return pulse_record
    except Exception as e:
        Log.warning(
            "Line {{index}}: Problem with line for key {{key}}\n{{line}}",
            line=line,
            index=i,
            key=source_key,
            cause=e
        )


def transform_buildbot(source_key, other, resources):
    output = Data()

    if other.what == "This is a heartbeat":
        return output

    output = bb.parse(other)

    if output.build.branch:
        rev = Revision(branch={"name": output.build.branch}, changeset=Changeset(id=output.build.revision))
        locale = output.build.locale.replace("en-US", DEFAULT_LOCALE)
        try:
            output.repo = minimize_repo(resources.hg.get_revision(rev, locale))
        except Exception as e:
            if "release-mozilla-esr" in e or "release-comm-esr" in e:
                # TODO: FIX PROBLEM WHERE, FOR SOME REASON, WE CAN NOT FIND THE REVISIONS FOR ESR
                pass
            else:
                Log.warning(
                    "Can not get revision for key={{key}}, branch={{branch}}, locale={{locale}}, revision={{revision}}\n{{details|json|indent}}",
                    key=source_key,
                    branch=output.build.branch,
                    locale=locale,
                    revision=rev,
                    details=output,
                    cause=e
                )

    else:
        bb.parse(other)
        Log.warning("No branch for {{key}}!\n{{output|indent}}", key=source_key, output=other)

    return output


def fix_locale(locale):
    # compensate for bug https://bugzilla.mozilla.org/show_bug.cgi?id=1174979
    if locale.find("\"") == -1:
        return locale
    return strings.between(locale, "\"", "\"")
