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

from collections import Mapping

import requests

from activedata_etl import etl2key
from activedata_etl.imports.resource_usage import normalize_resource_usage
from activedata_etl.imports.text_log import process_tc_live_log
from activedata_etl.transforms import TRY_AGAIN_LATER
from pyLibrary import convert
from pyLibrary.debugs.exceptions import suppress_exception, Except
from pyLibrary.debugs.logs import Log, machine_metadata
from pyLibrary.dot import set_default, Dict, unwraplist, listwrap, wrap
from pyLibrary.env import http
from pyLibrary.strings import expand_template
from pyLibrary.testing.fuzzytestcase import assertAlmostEqual
from pyLibrary.times.dates import Date

DEBUG = True
DISABLE_LOG_PARSING = False
MAX_THREADS = 5
STATUS_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
RETRY = {"times": 3, "sleep": 5}
seen_tasks = {}
new_seen_tc_properties = set()

def process(source_key, source, destination, resources, please_stop=None):
    output = []
    source_etl = None

    lines = list(enumerate(source.read_lines()))
    session = requests.session()
    for i, line in lines:
        if please_stop:
            Log.error("Shutdown detected. Stopping early")
        try:
            tc_message = convert.json2value(line)
            task_id = consume(tc_message, "status.taskId")
            etl = consume(tc_message, "etl")
            consume(tc_message, "_meta")

            Log.note("{{id}} found (line #{{num}})", id=task_id, num=i, artifact=tc_message.artifact.name)

            task_url = expand_template(STATUS_URL, {"task_id": task_id})
            task = http.get_json(task_url, retry=RETRY, session=session)
            if task.code == u'ResourceNotFound':
                Log.note("Can not find task {{task}} while processing key {{key}}", key=source_key, task=task_id)
                if not source_etl:
                    # USE ONE SOURCE ETL, OTHERWISE WE MAKE TOO MANY KEYS
                    source_etl = etl
                    if not source_etl.source.source:  # FIX ONCE TC LOGGER IS USING "tc" PREFIX FOR KEYS
                        source_etl.source.type = "join"
                        source_etl.source.source = {"id": "tc"}

                normalized = Dict(
                    task={"id": task_id},
                    etl={
                        "id": i,
                        "source": source_etl,
                        "type": "join",
                        "timestamp": Date.now(),
                        "error": "not found",
                        "machine": machine_metadata
                    }
                )

                output.append(normalized)

                continue
            normalized = _normalize(source_key, task_id, tc_message, task, resources)

            # get the artifact list for the taskId
            artifacts = normalized.task.artifacts = http.get_json(expand_template(ARTIFACTS_URL, {"task_id": task_id}), retry=RETRY).artifacts
            for a in artifacts:
                a.url = expand_template(ARTIFACT_URL, {"task_id": task_id, "path": a.name})
                a.expires = Date(a.expires)
                if a.name.endswith("/live.log"):
                    try:
                        read_actions(source_key, normalized, a.url)
                    except Exception, e:
                        if TRY_AGAIN_LATER in e:
                            Log.error("Aborting processing of {{key}}", key=source_key, cause=e)
                elif a.name.endswith("/resource-usage.json"):
                    with suppress_exception:
                        normalized.resource_usage = normalize_resource_usage(a.url)

            # FIX THE ETL
            if not source_etl:
                # USE ONE SOURCE ETL, OTHERWISE WE MAKE TOO MANY KEYS
                source_etl = etl
                if not source_etl.source.source:  # FIX ONCE TC LOGGER IS USING "tc" PREFIX FOR KEYS
                    source_etl.source.type = "join"
                    source_etl.source.source = {"id": "tc"}
            normalized.etl = {
                "id": i,
                "source": source_etl,
                "type": "join",
                "timestamp": Date.now(),
                "machine": machine_metadata
            }

            tc_message.artifact = "." if tc_message.artifact else None
            if normalized.task.id in seen_tasks:
                try:
                    assertAlmostEqual([tc_message, task, artifacts], seen_tasks[normalized.task.id], places=11)
                except Exception, e:
                    Log.error("Not expected", cause=e)
            else:
                tc_message._meta = None
                tc_message.runs = None
                tc_message.runId = None
                tc_message.artifact = None
                seen_tasks[normalized.task.id] = [tc_message, task, artifacts]

            output.append(normalized)
        except Exception, e:
            if TRY_AGAIN_LATER in e:
                raise e
            Log.warning("TaskCluster line not processed: {{line|quote}}", line=line, cause=e)

    keys = destination.extend({"id": etl2key(t.etl), "value": t} for t in output)
    return keys


def read_actions(source_key, normalized, url):
    if DISABLE_LOG_PARSING:
        return
    try:
        all_log_lines = http.get(url).get_all_lines(encoding=None)
        normalized.action = process_tc_live_log(all_log_lines, url, normalized)
    except Exception, e:
        e = Except.wrap(e)
        if "An existing connection was forcibly closed by the remote host" in e:
            Log.error(TRY_AGAIN_LATER, reason="text log was forcibly closed")
        else:
            Log.error("problem processing {{key}}", key=source_key, cause=e)


def _normalize(source_key, task_id, tc_message, task, resources):
    output = Dict()
    set_default(task, consume(tc_message, "status"))

    output.task.id = task_id
    output.task.created = Date(consume(task, "created"))
    output.task.deadline = Date(consume(task, "deadline"))
    output.task.dependencies = unwraplist(consume(task, "dependencies"))
    output.task.expires = Date(consume(task, "expires"))
    output.task.maxRunTime = consume(task, "payload.maxRunTime")

    env = consume(task, "payload.env")
    output.task.env = _object_to_array(env, "name", "value")

    features = consume(task, "payload.features")
    if all(isinstance(v, bool) for v in features.values()):
        output.task.features = [k if v else "!" + k for k, v in features.items()]
    else:
        Log.error("Unexpected features: {{features|json}}", features=features)
    output.task.cache = _object_to_array(consume(task, "payload.cache"), "name", "value")
    output.task.requires = consume(task, "requires")
    output.task.capabilities = consume(task, "payload.capabilities")

    image = consume(task, "payload.image")
    if isinstance(image, basestring):
        output.task.image = {"path": image}
    else:
        output.task.image = image

    output.task.priority = consume(task, "priority")
    output.task.provisioner.id = consume(task, "provisionerId")
    output.task.retries.remaining = consume(task, "retriesLeft")
    output.task.retries.total = consume(task, "retries")
    output.task.routes = consume(task, "routes")

    run_id = consume(tc_message, "runId")
    output.task.run = _normalize_run(task.runs[run_id])
    output.task.runs = map(_normalize_run, consume(task, "runs"))

    output.task.scheduler.id = consume(task, "schedulerId")
    output.task.scopes = consume(task, "scopes")
    output.task.state = consume(task, "state")
    output.task.group.id = consume(task, "taskGroupId")
    output.task.version = consume(tc_message, "version")
    output.task.worker.group = consume(tc_message, "workerGroup")
    output.task.worker.id = consume(tc_message, "workerId")
    output.task.worker.type = consume(task, "workerType")
    # DELETE JUNK
    consume(task, "payload.routes")

    artifacts = consume(task, "payload.artifacts")
    try:

        if isinstance(artifacts, list):
            for a in artifacts:
                if not a.name:
                    if not a.path:
                        Log.error("expecting name, or path of artifact")
                    else:
                        a.name = a.path
            output.task.artifacts = artifacts
        else:
            output.task.artifacts = unwraplist(_object_to_array(artifacts, "name"))
    except Exception, e:
        Log.warning("artifact format problem in {{key}}:\n{{artifact|json|indent}}", key=source_key, artifact=task.payload.artifacts, cause=e)
    output.task.cache = unwraplist(_object_to_array(task.payload.cache, "name", "path"))
    try:
        command = consume(task, "payload.command")
        cmd = consume(task, "payload.cmd")
        command = [cc for c in (command if command else cmd) for cc in listwrap(c)]   # SOMETIMES A LIST OF LISTS
        output.task.command = " ".join(map(convert.string2quote, map(unicode.strip, command)))
    except Exception, e:
        Log.error("problem", cause=e)

    set_build_info(source_key, output, task, env, resources)
    set_run_info(source_key, output, task, env)

    output.task.tags = get_tags(source_key, output.task.id, task)

    output.build.type = unwraplist(list(set(listwrap(output.build.type))))

    # ASSIGN TREEHERDER
    try:
        if output.build.revision and output.task.state != "exception":
            output.treeherder = resources.treeherder.get_markup(
                output.build.branch,
                output.build.revision,
                output.task.id,
                None,
                output.task.run.end_time
            )
    except Exception, e:
        if TRY_AGAIN_LATER in e:
            Log.error("Aborting processing of {{key}}", key=source_key, cause=e)

        Log.error(
            "Treeherder info could not be picked up for key={{key}}, revision={{revision}}",
            key=source_key,
            revision=output.build.revision12,
            cause=e
        )

    # PROPERTIES THAT HAVE NOT BEEN HANDLED
    remaining_keys = set([k for k, v in task.leaves()] + [k for k, v in tc_message.leaves()]) - new_seen_tc_properties
    if remaining_keys:
        map(new_seen_tc_properties.add, remaining_keys)
        Log.warning("Some properties ({{props|json}}) are not consumed while processing key {{key}}", key=source_key, props=remaining_keys)

    return output


def _normalize_run(run):
    output = Dict()
    output.reason_created = run.reasonCreated
    output.id = run.id
    output.scheduled = Date(run.scheduled)
    output.start_time = Date(run.started)
    output.end_time = Date(run.takenUntil)
    output.state = run.state
    output.worker.group = run.workerGroup
    output.worker.id = run.workerId
    return output


def set_run_info(source_key, normalized, task, env):
    """
    Get the run object that contains properties that describe the run of this job
    :param task: The task definition
    :return: The run object
    """
    metadata_name = consume(task, "metadata.name")
    set_default(
        normalized,
        {"run": {
            "key": consume(task, "payload.buildername"),
            "name": metadata_name,
            "machine": normalized.treeherder.machine,
            "suite": consume(task, "extra.suite"),
            "chunk": coalesce_w_conflict_detection(
                source_key,
                consume(task, "extra.chunks.current"),
                consume(task, "payload.properties.THIS_CHUNK")
            ),
            "timestamp": normalized.task.run.start_time
        }}
    )


def coalesce_w_conflict_detection(source_key, *args):
    output = None
    for a in args:
        if a == None:
            continue
        if output == None:
            output = a
        elif a != output:
            Log.warning("tried to coalesce {{values_|json}} while processing {{key}}", key=source_key, values_=args)
        else:
            pass
    return output


def set_build_info(source_key, normalized, task, env, resources):
    """
    Get a build object that describes the build
    :param task: The task definition
    :return: The build object
    """

    if task.workerType.startswith("dummy-type"):
        task.workerType = "dummy-type"

    build_type = consume(task, "extra.build_type")

    set_default(
        normalized,
        {"build": {
            "name": consume(task, "extra.build_name"),
            "product": coalesce_w_conflict_detection(
                source_key,
                consume(task, "payload.properties.product"),
                consume(task, "tags.build_props.product"),
                task.extra.treeherder.productName,
                consume(task, "extra.build_product")
            ),
            "platform": task.extra.treeherder.build.platform,
            # MOZILLA_BUILD_URL looks like this:
            # "https://queue.taskcluster.net/v1/task/e6TfNRfiR3W7ZbGS6SRGWg/artifacts/public/build/target.tar.bz2"
            "url": env.MOZILLA_BUILD_URL,
            "revision": coalesce_w_conflict_detection(
                source_key,
                consume(task, "tags.build_props.revision"),
                consume(task, "payload.sourcestamp.revision"),
                consume(task, "payload.properties.revision"),
                env.GECKO_HEAD_REV
            ),
            "type": listwrap({"dbg": "debug"}.get(build_type, build_type)),
            "version": consume(task, "tags.build_props.version"),
            "channel": consume(task, "payload.properties.channels")
        }}
    )

    normalized.build.branch = coalesce_w_conflict_detection(
        source_key,
        consume(task, "tags.build_props.branch"),
        consume(task, "payload.sourcestamp.branch").split("/")[-1],
        consume(task, "payload.properties.repo_path").split("/")[-1],
        env.GECKO_HEAD_REPOSITORY.split("/")[-2],   # will look like "https://hg.mozilla.org/try/"
        env.MH_BRANCH
    )
    normalized.build.revision12 = normalized.build.revision[0:12]

    if normalized.build.revision:
        normalized.repo = resources.hg.get_revision(wrap({"branch": {"name": normalized.build.branch}, "changeset": {"id": normalized.build.revision}}))
        normalized.build.date = normalized.repo.push.date

    treeherder = consume(task, "extra.treeherder")
    if treeherder:
        for l, v in treeherder.leaves():
            normalized.treeherder[l] = v

    for k, v in BUILD_TYPES.items():
        if treeherder.collection[k]:
            normalized.build.type += v

    diff = treeherder.collection.keys() - BUILD_TYPE_KEYS
    if diff:
        Log.warning("new collection type of {{type}} while processing key", type=diff, key=source_key)


def get_tags(source_key, task_id, task, parent=None):
    tags = []
    # SPECIAL CASES
    platforms = consume(task, "payload.properties.platforms")
    if isinstance(platforms, unicode):
        platforms = map(unicode.strip, platforms.split(","))
        tags.append({"name": "platforms", "value": platforms})

    # VARIOUS LOCATIONS TO FIND TAGS
    t = consume(task, "tags").leaves()
    m = consume(task, "metadata").leaves()
    e = consume(task, "extra").leaves()
    p = consume(task, "payload.properties").leaves()
    g = [(k, consume(task.payload, k)) for k in PAYLOAD_PROPERTIES]

    tags.extend({"name": k, "value": v} for k, v in t)
    tags.extend({"name": k, "value": v} for k, v in m)
    tags.extend({"name": k, "value": v} for k, v in e)
    tags.extend({"name": k, "value": v} for k, v in p)
    tags.extend({"name": k, "value": v} for k, v in g)

    clean_tags = []
    for t in tags:
        # ENSURE THE VALUES ARE UNICODE
        if parent:
            t['name'] = parent + "." + t['name']
        v = t["value"]
        if v == None:
            continue
        elif isinstance(v, list):
            if len(v) == 1:
                v = v[0]
                if isinstance(v, Mapping):
                    for tt in get_tags(source_key, task_id, Dict(tags=v), parent=t['name']):
                        clean_tags.append(tt)
                    continue
                elif not isinstance(v, unicode):
                    v = convert.value2json(v)
            # elif all(isinstance(vv, (unicode, float, int)) for vv in v):
            #     pass  # LIST OF PRIMITIVES IS OK
            else:
                v = convert.value2json(v)
        elif not isinstance(v, unicode):
            v = convert.value2json(v)
        t["value"] = v
        verify_tag(source_key, task_id, t)
        clean_tags.append(t)

    return clean_tags


def verify_tag(source_key, task_id, t):
    if not isinstance(t["value"], unicode):
        Log.error("Expecting unicode")
    if t["name"] not in KNOWN_TAGS:
        Log.warning("unknown task tag {{tag|quote}} while processing {{task_id}} in {{key}}", key=source_key, id=task_id, tag=t["name"])
        KNOWN_TAGS.add(t["name"])


def _scrub(record, name):
    value = record[name]
    record[name] = None
    if value == "-" or value == "":
        return None
    else:
        return unwraplist(value)


def _object_to_array(value, key_name, value_name=None):
    try:
        if value_name==None:
            return unwraplist([set_default(v, {key_name: k}) for k, v in value.items()])
        else:
            return unwraplist([{key_name: k, value_name: v} for k, v in value.items()])
    except Exception, e:
        Log.error("unexpected", cause=e)


BUILD_TYPES = {
    "arm-debug": ["debug", "arm"],
    "arm-opt": ["opt", "arm"],
    "asan": ["asan"],
    "ccov": ["ccov"],
    "debug": ["debug"],
    "gyp": ["gyp"],
    "jsdcov": ["jsdcov"],
    "lsan": ["lsan"],
    "memleak": ["memleak"],
    "opt": ["opt"],
    "pgo": ["pgo"],
    "nostylo": ["nostylo"],
    "ubsan": ["ubsan"]
}
BUILD_TYPE_KEYS = set(BUILD_TYPES.keys())

PAYLOAD_PROPERTIES = {
    "artifactsTaskId",
    "balrog_api_root",
    "build_number",
    "created",
    "deadline",
    "description",
    "desiredResolution",
    "encryptedEnv",
    "en_us_binary_url",
    "graphs",  # POINTER TO graph.json ARTIFACT
    "locales",
    "mar_tools_url",
    "next_version",
    "NO_BBCONFIG",
    "onExitStatus",
    "osGroups",
    "release_promotion",
    "repack_manifests_url",
    "script_repo_revision",
    "signingManifest",
    "supersederUrl",
    "taskid_to_beetmove"
    "template_key",
    "THIS_CHUNK",
    "TOTAL_CHUNKS",
    "tuxedo_server_url",
    "unsignedArtifacts",
    "upload_date",
    "VERIFY_CONFIG",
    "version"
}

KNOWN_TAGS = {
    "buildid",
    "build_name",
    "build_type",
    "build_product",
    "build_props.branch",
    "build_props.build_number",
    "build_props.locales",
    "build_props.mozharness_changeset",
    "build_props.partials",
    "build_props.platform",
    "build_props.product",
    "build_props.revision",
    "build_props.version",

    "chainOfTrust.inputs.docker-image",


    "chunks.current",
    "chunks.total",
    "crater.crateName",
    "crater.toolchain.customSha",
    "crater.crateVers",
    "crater.taskType",
    "crater.toolchain.archiveDate",
    "crater.toolchain.channel",
    "crater.toolchainGitRepo",
    "crater.toolchainGitSha",

    "createdForUser",
    "data.base.sha",
    "data.base.user.login",
    "data.head.sha",
    "data.head.user.email",
    "description",

    "extra.build_product",  # error?
    "funsize.partials",
    "funsize.partials.branch",
    "funsize.partials.from_mar",
    "funsize.partials.locale",
    "funsize.partials.platform",
    "funsize.partials.previousBuildNumber",
    "funsize.partials.previousVersion",
    "funsize.partials.to_mar",
    "funsize.partials.toBuildNumber",
    "funsize.partials.toVersion",
    "funsize.partials.update_number",

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
    "l10n_changesets",

    "locations.mozharness",
    "locations.test_packages",
    "locations.build",
    "locations.img",
    "locations.mar",
    "locations.sources",
    "locations.symbols",
    "locations.tests",
    "name",

    "notification.task-defined.irc.notify_nicks",
    "notification.task-defined.irc.message",
    "notification.task-defined.log_collect",
    "notification.task-defined.ses.body",
    "notification.task-defined.ses.recipients",
    "notification.task-defined.ses.subject",
    "notification.task-defined.smtp.body",
    "notification.task-defined.smtp.recipients",
    "notification.task-defined.smtp.subject",
    "notification.task-defined.sns.message",
    "notification.task-defined.sns.arn",

    "notifications.task-completed.message",
    "notifications.task-completed.ids",
    "notifications.task-completed.subject",
    "notifications.task-failed.message",
    "notifications.task-failed.ids",
    "notifications.task-failed.subject",
    "notifications.task-exception.message",
    "notifications.task-exception.ids",
    "notifications.task-exception.subject",

    "npmCache.url",
    "npmCache.expires",
    "objective",
    "owner",
    "parent_task_id",
    "partial_versions",
    "platforms",
    "signing.signature",
    "source",
    "suite.flavor",
    "suite.name",

    "treeherderEnv",
    "treeherder.build.platform",
    "treeherder.collection.ccov",
    "treeherder.collection.debug",
    "treeherder.collection.gyp",
    "treeherder.collection.jsdcov",
    "treeherder.collection.memleak",
    "treeherder.collection.opt",
    "treeherder.collection.pgo",
    "treeherder.collection.asan",
    "treeherder.collection.lsan",
    "treeherder.collection.arm-debug",
    "treeherder.collection.arm-opt",
    "treeherder.groupSymbol",
    "treeherder.groupName",
    "treeherder.jobKind",
    "treeherder.labels",
    "treeherder.machine.platform",
    "treeherder.productName",
    "treeherder.reason",
    "treeherder.revision",
    "treeherder.revision_hash",
    "treeherder.symbol",
    "treeherder.tier",


    "upload_to_task_id",
    "url.busybox",
    "useCloudMirror",
    "who"
} | PAYLOAD_PROPERTIES

def consume(props, key):
    output, props[key] = props[key], None
    return output
