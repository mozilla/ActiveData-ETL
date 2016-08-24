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

import requests

from activedata_etl.transforms import TRY_AGAIN_LATER
from pyLibrary import convert
from pyLibrary.debugs.exceptions import Except
from pyLibrary.debugs.logs import Log, machine_metadata
from pyLibrary.dot import coalesce, wrap, Dict, unwraplist, Null, DictList
from pyLibrary.env import http, elasticsearch
from pyLibrary.maths import Math
from pyLibrary.maths.randoms import Random
from pyLibrary.meta import cache, use_settings
from pyLibrary.queries import jx
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import HOUR, DAY, MINUTE, SECOND
from pyLibrary.times.timer import Timer

DEBUG = True
RESULT_SET_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/resultset/?format=json&count=1000&full=true&short_revision__in={{revision}}"
FAILURE_CLASSIFICATION_URL = "https://treeherder.mozilla.org/api/failureclassification/"
REPO_URL = "https://treeherder.mozilla.org:443/api/repository/"

JOBS_BY_REVISION = "https://treeherder.mozilla.org/api/project/{{branch}}/jobs/?count=2000&offset={{offset}}&result_set_id__in={{result_set_id}}"
JOBS_BY_JOB_ID = "https://treeherder.mozilla.org/api/project/{{branch}}/jobs/?count=2000&offset={{offset}}&job_id={{job_id}}"
DETAILS_BY_JOB_ID = "https://treeherder.mozilla.org/api/jobdetail/?job_id__in={{job_id}}&repository={{branch}}"
NOTES_BY_JOB_ID = "https://treeherder.mozilla.org/api/project/{{branch}}/note/?job_id={{job_id}}"
JOB_BUG_MAP_BY_JOB_ID = "https://treeherder.mozilla.org/api/project/{{branch}}/bug-job-map/?job_id={{job_id}}"


class TreeHerder(object):
    @use_settings
    def __init__(self, hg, use_cache=True, cache=None, rate_limiter=None, settings=None):
        cache.schema = SCHEMA
        rate_limiter.schema = RATE_LIMITER_SCHEMA

        self.settings = settings
        try:
            self.failure_classification = {c.id: c.name for c in http.get_json(FAILURE_CLASSIFICATION_URL)}
            self.repo = {c.id: c.name for c in http.get_json(REPO_URL)}
            self.disabled = False
            Log.warning("Treeherder ingestion is enabled!")
        except Exception:
            self.disabled = True

        self.hg = hg
        self.cache = elasticsearch.Cluster(cache).get_or_create_index(cache)
        self.rate_limiter = elasticsearch.Cluster(cache).get_or_create_index(rate_limiter)
        self.rate_limiter.set_refresh_interval(seconds=1)
        self.th_requests = []

    def _get_revision_results_from_th(self, branch, revision, job_id=None):
        """
        :param branch:
        :param revision:
        :return:  Null - IF THERE IS NOTHING, RAISE EXCEPTION IF WE SHOULD TRY AGAIN
        """

        if not job_id:
            if (branch, revision) in self.th_requests:
                Log.warning("Another request for same revsion!")
            self.th_requests.append((branch, revision))

        start = None
        request = Dict(last=Date.now(), count=0)

        def _register_call(end=None):
            try:
                request.count += 1
                _id = "-".join([branch, revision])
                if not start or end or request.last < Date.now() - 5 * SECOND:
                    if DEBUG:
                        Log.note("TH call count={{count}}", count=request.count)
                    request.last = Date.now()
                    response = http.put(
                        url=self.rate_limiter.url + "/" + _id,
                        timeout=3,
                        data=convert.unicode2utf8(convert.value2json(Dict(
                            machine=machine_metadata.name,
                            start=start,
                            last_seen=request.last,
                            end=end,
                            count=request.count
                        )))
                    )
                    if unicode(response.status_code)[0] != '2':
                        Log.error("Could not register call")
            except Exception, e:
                e = Except.wrap(e)
                if "Read timed out." in e or "Caused by ConnectTimeoutError" in e:
                    pass  # IT HAPPENS BECAUSE OF SHORT TIMEOUT, NO NEED TO FREAK OUT, OTHER CALLS WILL FAIL AND INFORM US OF PROBLEMS
                else:
                    Log.warning("Problem registering call", cause=e)

        def _get_job_details_from_th(revision_id=None, job_id=None):
            output = []
            jobs = DictList()

            with Timer("Get jobs", debug=DEBUG):
                while True:
                    if revision_id:
                        url = expand_template(JOBS_BY_REVISION, {"branch": branch, "offset": len(jobs), "result_set_id": unicode(revision_id)})
                    else:
                        url = expand_template(JOBS_BY_JOB_ID, {"branch": branch, "offset": len(jobs), "job_id": unicode(job_id)})

                    _register_call()
                    response = http.get_json(url)
                    jobs.extend(response.results)
                    if len(response.results) != 2000:
                        break

            with Timer("Get (up to {{num}}) details from TH", {"num": len(jobs)}, debug=DEBUG):
                details = []
                for _, ids in jx.groupby(jobs.id, size=40):
                    _register_call()
                    details.extend(http.get_json(
                        url=expand_template(DETAILS_BY_JOB_ID, {"branch": branch, "job_id": ",".join(map(unicode, ids))}),
                        retry={"times": 3}
                    ).results)
                details = {k.job_guid: list(v) for k, v in jx.groupby(details, "job_guid")}

            with Timer("Get (up to {{num}}) stars from TH", {"num": len(jobs)}, debug=DEBUG):
                stars = []
                for _, ids in jx.groupby(jobs.id, size=40):
                    _register_call()
                    response = http.get_json(expand_template(JOB_BUG_MAP_BY_JOB_ID, {"branch": branch, "job_id": "&job_id=".join(map(unicode, ids))}))
                    stars.extend(response),
                stars = {k.job_id: list(v) for k, v in jx.groupby(stars, "job_id")}

            with Timer("Get notes from TH", debug=DEBUG):
                notes = []
                for jid in set([j.id for j in jobs if j.failure_classification_id != 1] + stars.keys()):
                    _register_call()
                    response = http.get_json(expand_template(NOTES_BY_JOB_ID, {"branch": branch, "job_id": unicode(jid)}))
                    notes.extend(response),
                notes = {k.job_id: list(v) for k, v in jx.groupby(notes, "job_id")}

            for j in jobs:
                output.append(self._normalize_job_result(branch, revision, j, details, notes, stars))
            if output:
                with Timer("Write to ES cache"):
                    self.cache.extend({"id": "-".join([c.repo.branch, unicode(c.job.id)]), "value": c} for c in output)
                    try:
                        self.cache.refresh()
                    except Exception:
                        pass
            return output

        if job_id:
            return _get_job_details_from_th(job_id=job_id)

        # FIND THE JOBS FOR THIS REVISION
        try:
            url = expand_template(RESULT_SET_URL, {"branch": branch, "revision": revision[0:12:]})
            results = None
            for attempt in range(3):
                try:
                    _register_call()
                    start = coalesce(start, Date.now().unix)
                    response = http.get(url=url)
                    if str(response.status_code)[0] == b'2':
                        results = convert.json2value(convert.utf82unicode(response.content)).results
                        break
                    elif str(response.status_code)[0] == b'5':
                        # WE MAY HAVE CRUSHED TH
                        Log.error(TRY_AGAIN_LATER, reason="HTTP " + unicode(response.status) + " ERROR")
                    elif response.status_code == 404:
                        if branch not in ["hg.mozilla.org"]:
                            Log.warning("{{branch}} rev {{revision}} returns 404 NOT FOUND", branch=branch, revision=revision)
                        return Null
                    elif response.status_code == 403:
                        Log.error(TRY_AGAIN_LATER, reason="HTTP 403 ERROR")
                    else:
                        Log.warning("Do not know how to deal with TH error {{code}}", code=response.status_code)
                except Exception, e:
                    e = Except.wrap(e)
                    if "No JSON object could be decoded" not in e:
                        Log.error("Could not get good response from {{url}}", url=url, cause=e)

            if results is None:
                Log.error("Could not get good response from {{url}}", url=url, cause=e)

            return _get_job_details_from_th(revision_id=results[0].id)
        finally:
            _register_call(Date.now())

    def _normalize_job_result(self, branch, revision, job, details, notes, stars):
        output = Dict()
        try:
            job = wrap(copy(job))

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

            fcid = _scrub(job, "failure_classification_id")
            if fcid not in [0, 1]:  # 0 is unknown, and 1 is "not classified"
                output.job.failure_classification = self.failure_classification.get(fcid)

            if job.result_set:
                output.repo.push_date = job.result_set.push_timestamp
                output.repo.branch = self.repo[job.result_set.repository_id]
                output.repo.revision = job.result_set.revision
            else:
                output.repo.branch = branch
                output.repo.revision = revision
                output.repo.revision12 = revision[:12]
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

            # ATTACH DETAILS (AND SCRUB OUT REDUNDANT VALUES)
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
                            branch={"name": branch}
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
        except Exception, e:
            Log.error("Problem with normalization of job {{job_id}}", job_id=coalesce(output.job.id, job.id), cause=e)

    def _get_markup_from_es(self, branch, revision, task_id=None, buildername=None, timestamp=None):
        """
        :param branch:
        :param revision:
        :param task_id:
        :param buildername:
        :param timestamp:
        :return:
            Exception - Some logic problem
            None - Can not be found
            TH document - was found
            TODO:  A FOURTH STATE THAT INDICATES THE JOB NUMBER, SO WE CAN FOCUS ON LOOKING UP JUST THAT ONE JOB
        """
        if task_id:
            _filter = {"term": {"task.id": task_id}}
        elif buildername:
            _filter = {"term": {"ref_data_name": buildername}}
        else:
            Log.error("require buildername, or task_id")

        query = {
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and": [
                    _filter,
                    {"term": {"repo.branch": branch}},
                    {"prefix": {"repo.revision": revision}}
                ]}
            }},
            "size": 10000
        }

        docs = None
        for attempt in range(3):
            try:
                docs = self.cache.search(query, timeout=600).hits.hits
                break
            except Exception, e:
                e = Except.wrap(e)
                if "NodeNotConnectedException" in e or "NodeDisconnectedException" in e:
                    # WE LOST A NODE, THIS MAY TAKE A WHILE
                    Thread.sleep(seconds=Random.int(5 * 60))
                    continue
                elif "EsRejectedExecutionException[rejected execution (queue capacity" in e:
                    Thread.sleep(seconds=Random.int(30))
                    continue
                else:
                    Log.warning("Bad ES call, fall back to TH", cause=e)
                    return Null

        if not docs:
            if DEBUG:
                Log.note("No cached for {{value|quote}} rev {{revision}}", value=coalesce(task_id, buildername), revision=revision)
            return Null
        elif len(docs) == 1:
            if DEBUG:
                Log.note("Used ES cache to get TH details on {{value|quote}}", value=coalesce(task_id, buildername))
            return docs[0]._source
        elif timestamp == None:
            Log.error("timestamp required to find best match")
        else:
            # MISSING docs._source.job.timing.end WHEN A PLACEHOLDER WAS ADDED
            # TODO: SHOULD DELETE OVERAPPING PLACEHOLDER RECORDS
            if DEBUG:
                Log.note("Used ES cache to get TH details on {{value|quote}}", value=coalesce(task_id, buildername))
            timestamp = Date(timestamp).unix
            best_index = jx.sort([(i, abs(coalesce(e, 0) - timestamp)) for i, e in enumerate(docs._source.job.timing.end)], 1)[0][0]
            return docs[best_index]._source

    @cache(duration=HOUR, lock=True)
    def get_markup(self, branch, revision, task_id=None, buildername=None, timestamp=None):
        """
        THROW ERROR IF PROBLEM, OR IF THE RETURNED TH DETAIL IS STILL PENDING
        :param branch:
        :param revision:
        :param task_id:
        :param buildername:
        :param timestamp:
        :return:
        """

        # TRY CACHE
        if not branch or not revision:
            Log.error("expecting branch and revision")

        found_job_id = None

        while self.settings.use_cache:
            try:
                markup = self._get_markup_from_es(branch, revision, task_id, buildername, timestamp)

                if markup:
                    stale = markup.etl.timestamp < (Date.now() - HOUR).unix and \
                            markup.job.timing.last_modified > (Date.now() - DAY).unix

                    if stale:
                        markup = Null  # ALL OTHER JOBS IN REVISION ARE PROBABLY STALE
                    elif markup.job.state not in ["pending", "running"]:
                        # WE DO NOT NEED THESE DETAILS WHEN MARKING OTHER DOCUMENTS
                        markup.details = None
                        markup.stars = None
                        markup.notes = None
                        return markup

                found_job_id = markup.job.id
            except Exception, e:
                if "timestamp required to find best match" in e:
                    Log.error("Logic error", cause=e)

                Log.warning("can not get markup from es, check TH request logger next", cause=e)

            try:
                if self.disabled:
                    return None

                if self._is_it_safe_to_make_more_requests(branch, revision):
                    break
            except Exception, e:
                Log.warning("Problem using TH request logger", cause=e)
                continue

            Log.error(TRY_AGAIN_LATER, reason="Appear to be working on same revision")

        # REGISTER OUR TREEHERDER CALL
        job_results = self._get_revision_results_from_th(branch, revision, job_id=found_job_id)

        detail = Null
        for job_result in job_results:
            # MATCH TEST RUN BY UID DOES NOT EXIST, SO WE USE THE ARCANE BUILDER NAME
            # PLUS THE MATCHING START/END TIMES

            if job_result.build_system_type == "buildbot" and buildername != job_result.ref_data_name:
                continue
            if job_result.build_system_type == "taskcluster" and task_id != job_result.task.id:
                continue

            if detail == None:
                detail = job_result
            elif timestamp:
                timestamp = Date(timestamp).unix
                if abs(Date(detail.job.timing.end).unix - timestamp) < abs(Date(job_result.job.timing.end).unix - timestamp):
                    pass
                else:
                    detail = job_result
            else:
                Log.error("Not expecting more then one detail with no timestamp to help match")

        if detail.job.state == "pending":
            Log.error(TRY_AGAIN_LATER, reason="Treeherder not done ingesting")
        if not detail:
            # MAKE A FILLER RECORD FOR THE MISSING DATA
            detail = Dict()
            detail.ref_data_name = buildername
            detail.repo.branch = branch
            detail.repo.revision = revision
            detail.task.id = task_id
            detail.job.timing.last_modified = Date.now()
            detail.etl.timestamp = Date.now()

            self.cache.add({"value": detail})
            try:
                self.cache.refresh()
            except Exception:
                pass

        # WE DO NOT NEED THESE DETAILS WHEN MARKING OTHER DOCUMENTS
        detail.details = None
        detail.stars = None
        detail.notes = None
        return detail

    def _is_it_safe_to_make_more_requests(self, branch, revision):
        response = requests.post(
            url=self.rate_limiter.url + "/_search",
            timeout=self.rate_limiter.settings.timeout,
            data=convert.unicode2utf8(convert.value2json({
                "aggs": {"recent": {
                    "filter": {"range": {"start": {"gt": Date.now() - 5 * MINUTE}}},
                    "aggs": {
                        "requests": {"stats": {"field": "count"}},
                        "durations": {"stats": {"script_field": "doc[\"end\"].value-doc[\"start\"]"}}
                    }
                }},
                "size": 0
            }))
        )
        if response.status_code != 200:
            Log.error("bad return code {{code}}:\n{{data}}", code=response.status_code, data=response.content)
        summary = convert.json2value(convert.utf82unicode(response.content))
        request_count = summary.aggregations.recent.requests.sum
        if request_count > 4000:
            return False

        response = requests.get(
           url=self.rate_limiter.url + "/" + "-".join([branch, revision]),
           timeout=3
        )
        if response.status_code == 404:
            return True
        if response.status_code != 200:
            Log.error("bad return code {{code}}:\n{{data}}", code=response.status_code, data=response.content)
        last_th_request = convert.json2value(convert.utf82unicode(response.content))._source
        if last_th_request.machine == machine_metadata.name:
            # CATCH CASE WHEN TH LOOKUP COLLIDES WITH SELF (THE 2 MINUTE DELAY CAN CAUSE CONFUSION)
            return True
        expired = coalesce(
            last_th_request.last_seen + 2 * MINUTE.seconds,
            last_th_request.end + 10,
            last_th_request.start + 5 * MINUTE.seconds
        )
        now = Date.now().unix
        if expired < now:
            return True

        return False

    def replicate(self):
        # FIND HOLES IN JOBS
        # LOAD THEM FROM TREEHERDER
        pass


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



SCHEMA = {
    "settings": {
        "index.number_of_replicas": 1,
        "index.number_of_shards": 30
    },
    "mappings": {
        "job": {
            "_source": {
                "compress": True
            },
            "_id": {
                "index": "not_analyzed",
                "type": "string",
                "store": True
            },
            "_all": {
                "enabled": False
            },
            "dynamic_templates": [
                {
                    "default_ids": {
                        "mapping": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": True
                        },
                        "match": "id"
                    }
                },
                {
                    "default_strings": {
                        "mapping": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": True
                        },
                        "match_mapping_type": "string",
                        "match": "*"
                    }
                },
                {
                    "default_doubles": {
                        "mapping": {
                            "index": "not_analyzed",
                            "type": "double",
                            "doc_values": True
                        },
                        "match_mapping_type": "double",
                        "match": "*"
                    }
                },
                {
                    "default_longs": {
                        "mapping": {
                            "index": "not_analyzed",
                            "type": "long",
                            "doc_values": True
                        },
                        "match_mapping_type": "long|integer",
                        "match_pattern": "regex",
                        "path_match": ".*"
                    }
                }
            ],
            "properties": {
                "notes": {
                    "type": "nested"
                },
                "stars": {
                    "type": "nested"
                },
                "details": {
                    "type": "nested"
                }
            }
        },
        "etl": {
            "properties": {
                "timestamp": {
                    "index": "not_analyzed",
                    "type": "long",
                    "doc_values": True
                }
            }
        }
    }

}

RATE_LIMITER_SCHEMA = {
    "settings": {
        "index.number_of_replicas": 1,
        "index.number_of_shards": 1
    },
    "mappings": {
        "request": {
            "_ttl": {
                "enabled": True,
                "default": "2h"
            },
            "_id": {
                "index": "not_analyzed",
                "type": "string",
                "store": True
            }
        }
    }
}
