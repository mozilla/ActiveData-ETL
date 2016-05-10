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

import requests

from pyLibrary import convert
from pyLibrary.debugs.logs import Log, machine_metadata
from pyLibrary.dot import set_default, coalesce, Dict, unwraplist, listwrap, wrap
from pyLibrary.env import http
from pyLibrary.strings import expand_template
from pyLibrary.testing.fuzzytestcase import assertAlmostEqual
from pyLibrary.times.dates import Date
from testlog_etl import etl2key

DEBUG = True
MAX_THREADS = 5
STATUS_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
RETRY = {"times": 3, "sleep": 5}
seen = {}


def process(source_key, source, destination, resources, please_stop=None):
    output = []
    etl_source = None

    lines = source.read_lines()
    session = requests.session()
    for i, line in enumerate(lines):
        if please_stop:
            Log.error("Shutdown detected. Stopping early")
        try:
            tc_message = convert.json2value(line)
            taskid = tc_message.status.taskId
            if tc_message.artifact:
                continue
            Log.note("{{id}} found (line #{{num}})", id=taskid, num=i, artifact=tc_message.artifact.name)

            task = http.get_json(expand_template(STATUS_URL, {"task_id": taskid}), retry=RETRY, session=session)
            normalized = _normalize(source_key, tc_message, task)
            if normalized.build.revision:
                normalized.repo = resources.hg.get_revision(wrap({"branch": {"name": normalized.build.branch}, "changeset": {"id": normalized.build.revision}}))

            # get the artifact list for the taskId
            artifacts = http.get_json(expand_template(ARTIFACTS_URL, {"task_id": taskid}), retry=RETRY).artifacts
            for a in artifacts:
                a.url = expand_template(ARTIFACT_URL, {"task_id": taskid, "path": a.name})
                a.expires = Date(a.expires)
                if a.name.endswith("/live.log"):
                    read_buildbot_properties(normalized, a.url)
            normalized.task.artifacts = artifacts

             # FIX THE ETL
            etl = tc_message.etl
            etl_source = coalesce(etl_source, etl.source)
            etl.source = etl_source
            if not etl.source.source:  # FIX ONCE TC LOGGER IS USING "tc" PREFIX FOR KEYS
                etl.source.type = "join"
                etl.source.source = {"id": "tc"}
            normalized.etl = {
                "id": i,
                "source": etl,
                "type": "join",
                "timestamp": Date.now(),
                "machine": machine_metadata
            }

            tc_message.artifact = "." if tc_message.artifact else None
            if normalized.task.id in seen:
                try:
                    assertAlmostEqual([tc_message, task, artifacts], seen[normalized.task.id], places=11)
                except Exception, e:
                    Log.error("Not expected", cause=e)
            else:
                tc_message._meta = None
                tc_message.etl = None
                tc_message.artifact = None
                seen[normalized.task.id] = [tc_message, task, artifacts]

            output.append(normalized)
        except Exception, e:
            Log.warning("problem", cause=e)

    keys = destination.extend({"id": etl2key(t.etl), "value": t} for t in output)
    return keys


def read_buildbot_properties(normalized, url):
    pass
    # response = http.get(url)
    #
    # lines = list(response.all_lines)
    # for l in response.all_lines:
    #     pass



def _normalize(source_key, tc_message, task):
    output = Dict()
    set_default(task, tc_message.status)

    output.task.id = task.taskId
    output.task.created = Date(task.created)
    output.task.deadline = Date(task.deadline)
    output.task.dependencies = unwraplist(task.dependencies)
    output.task.env = task.payload.env
    output.task.expires = Date(task.expires)

    if isinstance(task.payload.image, basestring):
        output.task.image = {"path": task.payload.image}

    output.task.priority = task.priority
    output.task.provisioner.id = task.provisionerId
    output.task.retries.remaining = task.retriesLeft
    output.task.retries.total = task.retries
    output.task.routes = task.routes
    output.task.runs = map(_normalize_run, task.runs)
    output.task.run = _normalize_run(task.runs[tc_message.runId])
    if output.task.run.id != tc_message.runId:
        Log.error("not expected")

    output.task.scheduler.id = task.schedulerId
    output.task.scopes = task.scopes
    output.task.state = task.state
    output.task.group.id = task.taskGroupId
    output.task.version = tc_message.version
    output.task.worker.group = tc_message.workerGroup
    output.task.worker.id = tc_message.workerId
    output.task.worker.type = task.workerType

    try:
        output.task.artifacts = unwraplist(_object_to_array(task.payload.artifacts, "name"))
    except Exception, e:
        Log.warning("artifact format problem in {{key}}:\n{{artifact|json|indent}}", key=source_key, artifact=task.payload.artifacts, cause=e)
    output.task.cache = unwraplist(_object_to_array(task.payload.cache, "name", "path"))
    try:
        command = [cc for c in task.payload.command for cc in listwrap(c)]   # SOMETIMES A LIST OF LISTS
        output.task.command = " ".join(map(convert.string2quote, map(unicode.strip, command)))
    except Exception, e:
        Log.error("problem", cause=e)

    output.task.tags = get_tags(task)

    set_build_info(output, task)
    set_run_info(output, task)
    output.build.type = unwraplist(list(set(listwrap(output.build.type))))

    return output


def _normalize_run(run):
    output = Dict()
    output.reason_created = run.reasonCreated
    output.id = run.runId
    output.scheduled = Date(run.scheduled)
    output.started = Date(run.started)
    output.state = run.state
    output.deadline = Date(run.takenUntil)
    output.worker.group = run.workerGroup
    output.worker.id = run.workerId
    return output


def set_run_info(normalized, task):
    """
    Get the run object that contains properties that describe the run of this job
    :param task: The task definition
    :return: The run object
    """
    set_default(
        normalized,
        {"run": {
            "machine": task.extra.treeherder.machine,
            "suite": task.extra.suite,
            "chunk": task.extra.chunks.current
        }}
    )


def coalesce_w_conflict_detection(*args):
    output = None
    for a in args:
        if a == None:
            continue
        if output == None:
            output = a
        elif a != output:
            Log.warning("tried to coalesce {{values|json}}", values=args)
        else:
            pass
    return output


def set_build_info(normalized, task):
    """
    Get a build object that describes the build
    :param task: The task definition
    :return: The build object
    """

    if task.workerType.startswith("dummy-type"):
        task.workerType = "dummy-type"

    triple = (task.workerType, task.extra.build_name, task.extra.treeherder.build.platform)
    try:
        set_default(normalized, KNOWN_BUILD_NAMES[triple])
    except Exception:
        KNOWN_BUILD_NAMES[triple] = {}
        Log.warning("Can not find {{triple|json}}", triple=triple)

    set_default(
        normalized,
        {"build": {
            "name": task.extra.build_name,
            "product": coalesce_w_conflict_detection(
                task.tags.build_props.product,
                task.extra.treeherder.productName,
                task.extra.build_product
            ),
            "platform": task.extra.treeherder.build.platform,
            # MOZILLA_BUILD_URL looks like this:
            # "https://queue.taskcluster.net/v1/task/e6TfNRfiR3W7ZbGS6SRGWg/artifacts/public/build/target.tar.bz2"
            "url": task.payload.env.MOZILLA_BUILD_URL,
            "revision": coalesce_w_conflict_detection(
                task.tags.build_props.revision,
                task.payload.env.GECKO_HEAD_REV
            ),
            "type": listwrap({"dbg": "debug"}.get(task.extra.build_type, task.extra.build_type)),
            "version":task.tags.build_props.version
        }}
    )

    if task.extra.treeherder.collection.opt:
        normalized.build.type += ["opt"]
    elif task.extra.treeherder.collection.debug:
        normalized.build.type += ["debug"]

    # head_repo will look like "https://hg.mozilla.org/try/"
    head_repo = task.payload.env.GECKO_HEAD_REPOSITORY
    branch = head_repo.split("/")[-2]

    normalized.build.branch = coalesce_w_conflict_detection(
        branch,
        task.tags.build_props.branch
    )
    normalized.build.revision12 = normalized.build.revision[0:12]


def get_tags(task):
    tags = [{"name": k, "value": v} for k, v in task.tags.leaves()] + [{"name": k, "value": v} for k, v in task.metadata.leaves()] + [{"name": k, "value": v} for k, v in task.extra.leaves()]
    for t in tags:
        # ENSURE THE VALUES ARE UNICODE
        v = t["value"]
        if isinstance(v, list):
            if len(v) == 1:
                v = v[0]
            else:
                v = convert.value2json(v)
        elif not isinstance(v, unicode):
            v = convert.value2json(v)
        t["value"] = v

        if t["name"] not in KNOWN_TAGS:
            Log.warning("unknown task tag {{tag|quote}}", tag=t["name"])
            KNOWN_TAGS.add(t["name"])

    return unwraplist(tags)


def _object_to_array(value, key_name, value_name=None):
    try:
        if value_name==None:
            return [set_default(v, {key_name: k}) for k, v in value.items()]
        else:
            return [{key_name: k, value_name: v} for k, v in value.items()]
    except Exception, e:
        Log.error("unexpected", cause=e)


KNOWN_TAGS = {
    "build_name",
    "build_type",
    "build_product",
    "build_props.product",
    "build_props.build_number",
    "build_props.platform",
    "build_props.version",
    "build_props.branch",
    "build_props.locales",
    "build_props.revision",

    "chunks.current",
    "chunks.total",
    "crater.crateName",
    "crater.toolchain.customSha",
    "crater.crateVers",
    "crater.taskType",

    "createdForUser",
    "description",
    "extra.build_product",  # error?
    "funsize.partials",
    "github.branches",
    "github.events",
    "github.env",
    "github.headBranch",
    "github.headRepo",
    "github.headRevision",
    "github.headUser",
    "github.baseBranch",
    "github.baseRepo",
    "github.baseRevision",
    "github.baseUser",
    "githubPullRequest",

    "index.data.hello",
    "index.expires",
    "index.rank",
    "locations.mozharness",
    "locations.test_packages",
    "locations.build",
    "locations.img",
    "locations.sources",
    "locations.symbols",
    "locations.tests",
    "name",
    "npmCache.url",
    "npmCache.expires",
    "objective",
    "owner",
    "signing.signature",
    "source",
    "suite.flavor",
    "suite.name",
    "treeherderEnv",
    "treeherder.build.platform",
    "treeherder.collection.debug",
    "treeherder.collection.opt",
    "treeherder.collection.pgo",
    "treeherder.groupSymbol",
    "treeherder.groupName",
    "treeherder.machine.platform",
    "treeherder.productName",
    "treeherder.revision",
    "treeherder.revision_hash",
    "treeherder.symbol",
    "treeherder.tier",
    "url.busybox",
    "useCloudMirror"
}

# MAP TRIPLE (workerType, extra.build_name, extra.treeherder.build.platform)
# TO PROPERTIES
KNOWN_BUILD_NAMES = {
    ("android-api-15", "android-api-15-b2gdroid", "b2gdroid-4-0-armv7-api15"): {},
    ("android-api-15", "android-api-15-gradle-dependencies", "android-4-0-armv7-api15"): {},
    ("android-api-15", "android-checkstyle", "android-4-0-armv7-api15"): {},
    ("android-api-15", "android-lint", "android-4-0-armv7-api15"): {"build": {"platform": "lint"}},
    ("android-api-15", "android-api-15-partner-sample1", "android-4-0-armv7-api15-partner1"): {"run": {"machine": {"os": "android"}}},
    ("android-api-15", "android", "android-4-0-armv7-api15"): {"run": {"machine": {"os": "android"}}},
    ("android-api-15", "android-api-15-frontend", "android-4-0-armv7-api15"): {"run": {"machine": {"os": "android"}}},
    ("b2gtest", "mozharness-tox", "lint"): {},
    # ("b2gtest", "marionette-harness-pytest", "linux64"): {},
    ("b2gtest", None, None): {},
    ("b2gtest", None, "mulet-linux64"): {"build": {"platform": "linux64", "type": ["mulet"]}},
    ("b2gtest", "", "lint"): {"build": {"platform": "lint"}},
    ("b2gtest", "eslint-gecko", "lint"): {"build": {"platform": "lint"}},
    ("b2gtest", "marionette-harness-pytest", "linux64"): {},
    ("b2gtest-emulator", None, "b2g-emu-x86-kk"): {"run": {"machine": {"type": "emulator"}}},

    ("b2gbuild", None, "mulet-linux64"): {},

    ("buildbot", None, None): {},
    ("buildbot-try", None, None): {},
    ("buildbot-bridge", None, None): {},
    ("buildbot-bridge", None, "all"): {},
    ("cratertest", None, None): {},

    ("dbg-linux64", "browser-haz", "linux64"):{"run": {"machine": {"os": "linux64"}}, "build": {"type": ["debug", "hazard"]}},
    ("dbg-linux32", "linux32", "linux32"): {"run": {"machine": {"os": "linux32"}}, "build": {"type": ["debug"]}},
    ("dbg-linux64", "linux64", "linux64"): {"run": {"machine": {"os": "linux64"}}, "build": {"type": ["debug"]}},
    ("dbg-macosx64", "macosx64", "osx-10-7"): {"build": {"os": "macosx64"}},
    ("dbg-linux64", "shell-haz", "linux64"): {},

    ("desktop-test", None, "linux64"): {"build": {"platform": "linux64"}},
    ("desktop-test-xlarge", None, "linux64"): {"build": {"platform": "linux64"}},
    ("desktop-test-xlarge", "marionette-harness-pytest", "linux64"): {"build": {"platform": "linux64"}},
    ("dolphin", "dolphin-eng", "b2g-device-image"): {},
    ("dummy-type", None, None): {},
    ("emulator-ics", "emulator-ics", "b2g-emu-ics"): {"run": {"machine": {"type": "emulator"}}},
    ("emulator-ics-debug", "emulator-ics", "b2g-emu-ics"): {"run": {"machine": {"type": "emulator"}}, "build": {"type": ["debug"]}},
    ("emulator-jb", None, None): {},
    ("emulator-jb", "emulator-jb", "b2g-emu-jb"): {"run": {"machine": {"type": "emulator"}}},
    ("emulator-jb-debug", "emulator-jb", "b2g-emu-jb"): {"run": {"machine": {"type": "emulator"}}, "build": {"type": ["debug"]}},
    ("emulator-kk", "emulator-kk", "b2g-emu-kk"): {"run": {"machine": {"type": "emulator"}}},
    ("emulator-kk-debug", "emulator-kk", "b2g-emu-kk"): {"run": {"machine": {"type": "emulator"}}},
    ("emulator-l", "emulator-l", "b2g-emu-l"): {"run": {"machine": {"type": "emulator"}}},
    ("emulator-l-debug", "emulator-l", "b2g-emu-l"): {"run": {"machine": {"type": "emulator"}}, "build": {"type": ["debug"]}},
    ("emulator-x86-kk", "emulator-x86-kk", "b2g-emu-x86-kk"): {"run": {"machine": {"type": "emulator"}}},

    ("flame-kk", "aries", "b2g-device-image"): {"run": {"machine": {"type": "aries"}}},
    ("flame-kk", "aries-eng", "b2g-device-image"): {"run": {"machine": {"type": "aries"}}},
    ("flame-kk", "aries-noril", "b2g-device-image"): {"run": {"machine": {"type": "aries"}}},
    ("flame-kk", "flame-kk", "b2g-device-image"): {"run": {"machine": {"type": "flame"}}},
    ("flame-kk", "flame-kk-eng", "b2g-device-image"): {"run": {"machine": {"type": "flame"}}},
    ("flame-kk", "flame-kk-spark-eng", "b2g-device-image"): {"run": {"machine": {"type": "flame"}}},
    ("flame-kk", "nexus-5-user", "b2g-device-image"):{"run": {"machine": {"type": "nexus"}}},

    ("flame-kk", "nexus-4-eng", "b2g-device-image"): {"run": {"machine": {"type": "nexus4"}}},
    ("flame-kk", "nexus-4-kk-eng", "b2g-device-image"): {"run": {"machine": {"type": "nexus4"}}},
    ("flame-kk", "nexus-4-kk-user", "b2g-device-image"): {"run": {"machine": {"type": "nexus4"}}},
    ("flame-kk", "nexus-4-user", "b2g-device-image"): {"run": {"machine": {"type": "nexus4"}}},
    ("flame-kk", "nexus-5-l-eng", "b2g-device-image"): {"run": {"machine": {"type": "nexus5"}}},

    ("funsize-mar-generator", None, "osx-10-10"): {},
    ("funsize-mar-generator", None, "linux64"): {},
    ("funsize-mar-generator", None, "linux32"): {},
    ("funsize-mar-generator", None, "windowsxp"): {},
    ("funsize-mar-generator", None, "windows8-64"): {},
    ("funsize-balrog", None, "osx-10-10"): {},
    ("funsize-balrog", None, "linux32"): {},
    ("funsize-balrog", None, "linux64"): {},
    ("funsize-balrog", None, "windowsxp"):{},
    ("funsize-balrog", None, "windows8-64"):{},
    ("gaia", None, None): {},
    ("gecko-decision", None, None): {},
    ("github-worker", None, None): {},
    ("human-decision", None, None): {},

    ("mulet-opt", "mulet", "mulet-linux64"): {"build": {"platform": "linux64", "type": ["mulet", "opt"]}},
    ("opt-linux32", "linux32", "linux32"): {"run": {"machine": {"os": "linux32"}}, "build": {"platform": "linux32", "type": ["opt"]}},
    ("opt-linux64", None, None): {"build": {"platform": "linux64", "type": ["opt"]}},
    ("opt-linux64", None, "linux32"): {},
    ("opt-linux64", None, "linux64"): {"build": {"platform": "linux64", "type": ["opt"]}},
    ("opt-linux64", None, "osx-10-10"): {},
    ("opt-linux64", None, "windowsxp"): {},
    ("opt-linux64", None, "windows8-64"): {},
    ("opt-linux64", "linux64", "linux64"): {"run": {"machine": {"os": "linux64"}}, "build": {"platform": "linux64", "type": ["opt"]}},
    ("opt-linux64", "linux64-artifact", "linux64"): {"build": {"platform": "linux64", "type": ["opt"]}},

    ("opt-linux64", "linux64-gcc", "linux64"): {"build": {"platform": "linux64", "type": ["opt"], "compiler": "gcc"}},
    ("opt-linux64", "linux64-st-an", "linux64"): {"run": {"machine": {"os": "linux64"}}, "build": {"type": ["static analysis", "opt"]}},

    ("opt-macosx64", "macosx64", "osx-10-7"): {"build": {"os": "macosx64", "type": ["opt"]}},
    ("opt-macosx64", "macosx64-st-an", "osx-10-7"): {"build": {"os": "macosx64", "type": ["opt", "static analysis"]}},
    ("rustbuild", None, None): {},
    ("signing-worker-v1", None, "linux32"): {},
    ("signing-worker-v1", None, "osx-10-10"): {},
    ("signing-worker-v1", None, "linux64"): {},
    ("signing-worker-v1", None, "windowsxp"): {},
    ("signing-worker-v1", None, "windows8-64"): {},
    ("spidermonkey", "sm-arm-sim", "linux64"): {},
    ("spidermonkey", "sm-compacting", "linux64"):{},
    ("spidermonkey", "sm-rootanalysis", "linux64"): {},
    ("spidermonkey", "sm-plain", "linux64"): {"build": {"product": "spidermonkey", "platform": "linux64"}},
    ("spidermonkey", "sm-plaindebug", "linux64"): {"build": {"product": "spidermonkey", "platform": "linux64", "type": ["debug"]}},
    ("spidermonkey", "sm-generational", "linux64"): {},
    ("spidermonkey", "sm-warnaserr", "linux64"): {"build": {"product": "spidermonkey", "platform": "linux64", "type": ["debug"]}},
    ("spidermonkey", "sm-warnaserrdebug", "linux64"): {"build": {"product": "spidermonkey", "platform": "linux64", "type": ["debug"]}},

    ("symbol-upload", None, None): {},
    ("symbol-upload", None, "linux64"): {},
    ("symbol-upload", None, "android-4-0-armv7-api15"): {},
    ("taskcluster-images", None, "all"): {},
    ("taskcluster-images", None, "taskcluster-images"): {},
    ("tcvcs-cache-device", None, None): {},
    ("tutorial", None, None): {},
    ("worker-ci-test", None, None): {}

}
