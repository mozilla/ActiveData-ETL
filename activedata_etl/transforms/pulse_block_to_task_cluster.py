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

from activedata_etl.transforms import TRY_AGAIN_LATER
from pyLibrary import convert
from pyLibrary.debugs.logs import Log, machine_metadata
from pyLibrary.dot import set_default, Dict, unwraplist, listwrap, wrap
from pyLibrary.env import http
from pyLibrary.strings import expand_template
from pyLibrary.testing.fuzzytestcase import assertAlmostEqual
from pyLibrary.times.dates import Date
from activedata_etl import etl2key

DEBUG = True
MAX_THREADS = 5
STATUS_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}"
ARTIFACTS_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}/artifacts"
ARTIFACT_URL = "http://queue.taskcluster.net/v1/task/{{task_id}}/artifacts/{{path}}"
RETRY = {"times": 3, "sleep": 5}
seen = {}


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
            taskid = tc_message.status.taskId
            Log.note("{{id}} found (line #{{num}})", id=taskid, num=i, artifact=tc_message.artifact.name)

            task = http.get_json(expand_template(STATUS_URL, {"task_id": taskid}), retry=RETRY, session=session)
            normalized = _normalize(source_key, tc_message, task, resources)

            # get the artifact list for the taskId
            artifacts = http.get_json(expand_template(ARTIFACTS_URL, {"task_id": taskid}), retry=RETRY).artifacts
            for a in artifacts:
                a.url = expand_template(ARTIFACT_URL, {"task_id": taskid, "path": a.name})
                a.expires = Date(a.expires)
                if a.name.endswith("/live.log"):
                    read_buildbot_properties(normalized, a.url)
            normalized.task.artifacts = artifacts

            # FIX THE ETL
            if not source_etl:
                # USE ONE SOURCE ETL, OTHERWISE WE MAKE TOO MANY KEYS
                source_etl = tc_message.etl
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
            if TRY_AGAIN_LATER in e:
                raise e
            Log.warning("TaskCluster line not processed: {{line|quote}}", line=line, cause=e)

    keys = destination.extend({"id": etl2key(t.etl), "value": t} for t in output)
    return keys


def read_buildbot_properties(normalized, url):
    pass
    # response = http.get(url)
    #
    # lines = list(response.all_lines)
    # for l in response.all_lines:
    #     pass


def _normalize(source_key, tc_message, task, resources):
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
        if isinstance(task.payload.artifacts, list):
            for a in task.payload.artifacts:
                if not a.name:
                    if not a.path:
                        Log.error("expecting name, or path of artifact")
                    else:
                        a.name = a.path
            output.task.artifacts = task.payload.artifacts
        else:
            output.task.artifacts = unwraplist(_object_to_array(task.payload.artifacts, "name"))
    except Exception, e:
        Log.warning("artifact format problem in {{key}}:\n{{artifact|json|indent}}", key=source_key, artifact=task.payload.artifacts, cause=e)
    output.task.cache = unwraplist(_object_to_array(task.payload.cache, "name", "path"))
    try:
        command = [cc for c in task.payload.command for cc in listwrap(c)]   # SOMETIMES A LIST OF LISTS
        output.task.command = " ".join(map(convert.string2quote, map(unicode.strip, command)))
    except Exception, e:
        Log.error("problem", cause=e)

    output.task.tags = get_tags(source_key, task)

    set_build_info(output, task, resources)
    set_run_info(output, task)
    output.build.type = unwraplist(list(set(listwrap(output.build.type))))

    try:
        if output.build.revision :
            output.treeherder = resources.treeherder.get_markup(
                output.build.branch,
                output.build.revision,
                output.task.id,
                None,
                output.task.run.end_time
            )
    except Exception, e:
        if task.state == "exception":
            Log.note("Exception in {{task_id}}", task_id=output.task.id)
            return output

        if TRY_AGAIN_LATER in e:
            Log.error("Aborting processing of {{key}}", key=source_key, cause=e)

        Log.error(
            "Treeherder info could not be picked up for key={{key}}, revision={{revision}}",
            key=source_key,
            revision=output.build.revision12,
            cause=e
        )

    return output


def _normalize_run(run):
    output = Dict()
    output.reason_created = run.reasonCreated
    output.id = run.runId
    output.scheduled = Date(run.scheduled)
    output.start_time = Date(run.started)
    output.end_time = Date(run.takenUntil)
    output.state = run.state
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
            "chunk": task.extra.chunks.current,
            "timestamp": task.run.start_time
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


def set_build_info(normalized, task, resources):
    """
    Get a build object that describes the build
    :param task: The task definition
    :return: The build object
    """

    if task.workerType.startswith("dummy-type"):
        task.workerType = "dummy-type"

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
            "version": task.tags.build_props.version
        }}
    )

    if normalized.build.revision:
        normalized.repo = resources.hg.get_revision(wrap({"branch": {"name": normalized.build.branch}, "changeset": {"id": normalized.build.revision}}))
        normalized.build.date = normalized.repo.push.date

    if task.extra.treeherder:
        for l, v in task.extra.treeherder.leaves():
            normalized.treeherder[l] = v

    for k, v in BUILD_TYPES.items():
        if task.extra.treeherder.collection[k]:
            normalized.build.type += v

    # head_repo will look like "https://hg.mozilla.org/try/"
    head_repo = task.payload.env.GECKO_HEAD_REPOSITORY
    branch = head_repo.split("/")[-2]

    normalized.build.branch = coalesce_w_conflict_detection(
        branch,
        task.tags.build_props.branch
    )
    normalized.build.revision12 = normalized.build.revision[0:12]


def get_tags(source_key, task, parent=None):
    tags = [{"name": k, "value": v} for k, v in task.tags.leaves()] + [{"name": k, "value": v} for k, v in task.metadata.leaves()] + [{"name": k, "value": v} for k, v in task.extra.leaves()]
    clean_tags = []
    for t in tags:
        # ENSURE THE VALUES ARE UNICODE
        if parent:
            t['name'] = parent + "." + t['name']
        v = t["value"]
        if isinstance(v, list):
            if len(v) == 1:
                v = v[0]
                if isinstance(v, Mapping):
                    for tt in get_tags(source_key, Dict(tags=v), parent=t['name']):
                        clean_tags.append(tt)
                    continue
                elif not isinstance(v, unicode):
                    v = convert.value2json(v)
            else:
                v = convert.value2json(v)
        elif not isinstance(v, unicode):
            v = convert.value2json(v)
        t["value"] = v
        verify_tag(source_key, t)
        clean_tags.append(t)

    return clean_tags


def verify_tag(source_key, t):
    if not isinstance(t["value"], unicode):
        Log.error("Expecting unicode")
    if t["name"] not in KNOWN_TAGS:
        Log.warning("unknown task tag {{tag|quote}} while processing {{key}}", key=source_key, tag=t["name"])
        KNOWN_TAGS.add(t["name"])


def _object_to_array(value, key_name, value_name=None):
    try:
        if value_name==None:
            return [set_default(v, {key_name: k}) for k, v in value.items()]
        else:
            return [{key_name: k, value_name: v} for k, v in value.items()]
    except Exception, e:
        Log.error("unexpected", cause=e)

BUILD_TYPES = {
    "opt": ["opt"],
    "debug": ["debug"],
    "asan": ["asan"],
    "pgo": ["pgo"],
    "lsan": ["lsan"],
    "memleak": ["memleak"],
    "arm-debug": ["debug", "arm"],
    "arm-opt": ["opt", "arm"]
}


KNOWN_TAGS = {
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
    "signing.signature",
    "source",
    "suite.flavor",
    "suite.name",

    "treeherderEnv",
    "treeherder.build.platform",
    "treeherder.collection.debug",
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
    "treeherder.revision",
    "treeherder.revision_hash",
    "treeherder.symbol",
    "treeherder.tier",



    "url.busybox",
    "useCloudMirror"
}

