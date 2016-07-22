# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import re
from copy import copy

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap, Dict, unwraplist, Null
from pyLibrary.env import http, elasticsearch
from pyLibrary.maths import Math
from pyLibrary.meta import cache, use_settings
from pyLibrary.queries import jx
from pyLibrary.strings import expand_template
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import HOUR, DAY

RESULT_SET_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/resultset/?format=json&count=1000&full=true&short_revision__in={{revision}}"
FAILURE_CLASSIFICATION_URL = "https://treeherder.mozilla.org/api/failureclassification/"
REPO_URL = "https://treeherder.mozilla.org:443/api/repository/"
JOBS_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/jobs/?count=2000&result_set_id__in={{result_set_id}}"

DETAILS_URL = "https://treeherder.mozilla.org/api/jobdetail/?job_id__in={{job_id}}&repository={{branch}}"
NOTES_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/note/?job_id={{job_id}}"
JOB_BUG_MAP = "https://treeherder.mozilla.org/api/project/{{branch}}/bug-job-map/?job_id__in={{job_id}}"


class TreeHerder(object):
    @use_settings
    def __init__(self, hg, use_cache=True, settings=None):
        self.settings = settings
        self.failure_classification = {c.id: c.name for c in http.get_json(FAILURE_CLASSIFICATION_URL)}
        self.repo = {c.id: c.name for c in http.get_json(REPO_URL)}
        self.hg = hg
        self.cache = elasticsearch.Cluster(settings.elasticsearch).get_or_create_index(settings.elasticsearch)

    def _get_job_results_from_th(self, branch, revision):
        results = http.get_json(expand_template(RESULT_SET_URL, {"branch": branch, "revision": revision[0:12:]})).results

        output = []
        for g, repo_ids in jx.groupby(results.id, size=10):
            jobs = http.get_json(expand_template(JOBS_URL, {"branch": branch, "result_set_id": ",".join(map(unicode, repo_ids))})).results

            details = []
            for _, ids in jx.groupby(jobs.id, size=40):
                details.extend(http.get_json(
                    url=expand_template(DETAILS_URL, {"branch": branch, "job_id": ",".join(map(unicode, ids))}),
                    retry={"times": 3}
                ).results)
            details = {k.job_guid: list(v) for k, v in jx.groupby(details, "job_guid")}

            stars = []
            for _, ids in jx.groupby(jobs.id, size=40):
                response = http.get_json(expand_template(JOB_BUG_MAP, {"branch": branch, "job_id": ",".join(map(unicode, ids))}))
                stars.extend(response),
            stars = {k.job_id: list(v) for k, v in jx.groupby(stars, "job_id")}

            notes = []
            for jid in set([j.id for j in jobs if j.failure_classification_id != 1] + stars.keys()):
                response = http.get_json(expand_template(NOTES_URL, {"branch": branch, "job_id": unicode(jid)}))
                notes.extend(response),
            notes = {k.job_id: list(v) for k, v in jx.groupby(notes, "job_id")}

            for j in jobs:
                output.append(self._normalize_job_result(branch, revision, j, details, notes, stars))
        if output:
            self.cache.extend({"id": "-".join([c.repo.branch, unicode(c.job.id)]), "value": c} for c in output)
            self.cache.flush()
        return output

    def _normalize_job_result(self, branch, revision, job, details, notes, stars):
        job = wrap(copy(job))
        output = Dict()

        # ORGANIZE PROPERTIES
        output.build.architecture = _scrub(job, "build_architecture")
        output.build.os = _scrub(job, "build_os")
        output.build.platform = _scrub(job, "build_platform")
        output.build.type = _scrub(job, "platform_option")

        output.build_system_type = _scrub(job, "build_system_type")

        output.job.id = _scrub(job, "id")
        output.job.guid = _scrub(job, "job_guid")
        if job.job_group_symbol != "?":
            output.job.group.name = _scrub(job, "job_group_name")
            output.job.group.description = _scrub(job, "job_group_description")
            output.job.group.symbol = _scrub(job, "job_group_symbol")
        else:
            job.job_group_name = None
            job.job_group_description = None
            job.job_group_symbol = None
        output.job.type.description = _scrub(job, "job_type_description")
        output.job.type.name = _scrub(job, "job_type_name")
        output.job.type.symbol = _scrub(job, "job_type_symbol")

        output.ref_data_name = _scrub(job, "ref_data_name")

        output.machine.name = _scrub(job, "machine_name")
        if Math.is_integer(output.machine.name.split("-")[-1]):
            output.machine.pool = "-".join(output.machine.name.split("-")[:-1])
        output.machine.platform = _scrub(job, "machine_platform_architecture")
        output.machine.os = _scrub(job, "machine_platform_os")

        output.job.reason = _scrub(job, "reason")
        output.job.state = _scrub(job, "state")
        output.job.tier = _scrub(job, "tier")
        output.job.who = _scrub(job, "who")
        output.job.result = _scrub(job, "result")

        output.job.failure_classification = self.failure_classification[_scrub(job, "failure_classification_id")]

        if job.result_set:
            output.repo.push_date = job.result_set.push_timestamp
            output.repo.branch = self.repo[job.result_set.repository_id]
            output.repo.revision = job.result_set.revision
        else:
            output.repo.branch = branch
            output.repo.revision = revision
            output.repo.revision12=revision[:12]
        output.job.timing.submit = Date(_scrub(job, "submit_timestamp"))
        output.job.timing.start = Date(_scrub(job, "start_timestamp"))
        output.job.timing.end = Date(_scrub(job, "end_timestamp"))
        output.job.timing.last_modified = Date(_scrub(job, "last_modified"))

        # IGNORED
        job.job_group_id = None
        job.job_type_id = None
        job.result_set = None
        job.build_platform_id = None
        job.job_coalesced_to_guid = None
        job.option_collection_hash = None
        job.platform = None
        job.result_set_id = None
        job.running_eta = None
        job.signature = None

        if job.keys():
            Log.error("{{names|json}} are not used", names=job.keys())

        # ATTACH DETAILS (AND SCRUB OUT REDUNDANT VALUES
        output.details = details.get(output.job.guid, Null)
        for d in output.details:
            d.job_guid = None
            d.job_id = None

        output.task.id = coalesce(*map(_extract_task_id, output.details.url))

        # ATTACH NOTES (RESOLVED BY BUG...)
        for n in notes.get(output.job.id, Null):
            note = coalesce(n.note.strip(), n.text.strip())
            if note:
                # LOOK UP REVISION IN REPO
                fix = re.findall(r'[0-9A-Fa-f]{12}', note)
                if fix:
                    rev = self.hg.get_revision(Dict(
                        changeset={"id": fix[0]},
                        branch={"name": job.build.branch}
                    ))
                    n.revision = rev.changeset.id
                    n.bug_id = self.hg._extract_bug_id(rev.changeset.description)
            else:
                note = None

            output.notes += [{
                "note": note,
                "status": coalesce(n.active_status, n.status),
                "revision": n.revision,
                "bug_id": n.bug_id,
                "who": n.who,
                "failure_classification": self.failure_classification[n.failure_classification_id],
                "timestamp": Date(coalesce(n.note_timestamp, n.timestamp, n.created))
            }]

        # ATTACH STAR INFO
        for s in stars.get(output.job.id, Null):
            # LOOKUP BUG DETAILS
            output.stars += [{
                "bug_id": s.bug_id,
                "who": s.who,
                "timestamp": s.submit_timestamp
            }]

        output.etl = {"timestamp": Date.now()}
        return output

    @cache(duration=HOUR)
    def get_markup(self, branch, revision, task_id=None, buildername=None):
        # TRY CACHE
        if not branch or not revision:
            Log.error("expecting branch and revision")

        if self.settings.use_cache:
            if task_id:
                _filter = {"term": {"task.id": task_id}}
            else:
                _filter = {"term": {"ref_data_name": buildername}}

            query = {
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [
                        _filter,
                        {"term": {"repo.branch": branch}},
                        {"prefix": {"repo.revision": revision}},
                        {"or": [
                            {"range": {"etl.timestamp": {"gte": (Date.now() - HOUR).unix}}},
                            {"range": {"job.timing.last_modified": {"lt": (Date.now() - DAY).unix}}}
                        ]}
                    ]}
                }},
                "size": 10000,
            }
            try:
                docs = self.cache.search(query, timeout=120).hits.hits
                if not docs:
                    convert.value2json(query)
                    pass
                elif len(docs) == 1:
                    Log.note("Used ES cache to get details on {{value}}", value=coalesce(task_id, buildername))
                    return docs[0]._source
                else:
                    Log.warning("expecting no more than one document")
            except Exception, e:
                Log.warning("Bad ES call, fall back to TH", e)

        detail = None
        job_results = self._get_job_results_from_th(branch, revision)
        for job_result in job_results:
            # MATCH TEST RUN BY UID DOES NOT EXIST, SO WE USE THE ARCANE BUILDER NAME
            # PLUS THE MATCHING START/END TIMES

            if job_result.build_system_type == "buildbot" and buildername != job_result.ref_data_name:
                continue
            if job_result.build_system_type == "taskcluster" and task_id != job_result.task.id:
                continue

            if detail is not None:
                Log.error("Expecting only one match!")

            detail = job_result

        return detail


def _scrub(record, name):
    value = record[name]
    record[name] = None
    if value == "-" or value == "":
        return None
    else:
        return unwraplist(value)


def _extract_task_id(url):

    if "taskcluster.net" not in url:
        return None

    try:
        task_id = re.findall(r"[\w-]{22}", url)[0]
        return task_id
    except Exception:
        return None


