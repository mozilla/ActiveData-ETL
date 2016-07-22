import re
from copy import copy

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap, unwrap, Dict, unwraplist
from pyLibrary.env import http
from pyLibrary.maths import Math
from pyLibrary.meta import cache, use_settings
from pyLibrary.strings import expand_template
from pyLibrary.times.dates import Date

RESULT_SET_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/resultset/?format=json&full=true&revision__in={{revision}}"
JOBS_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/jobs/?count=2000&result_set_id__in={{result_set_id}}&failure_classification_id__ne=1"
NOTES_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/note/?job_id={{job_id}}"
JOB_BUG_MAP = "https://treeherder.mozilla.org/api/project/{{branch}}/bug-job-map/?job_id={{job_id}}"
FAILURE_CLASSIFICATION_URL = "https://treeherder.mozilla.org/api/failureclassification/"


class TreeHerder(object):
    @use_settings
    def __init__(self, hg, timeout=None, use_cache=True, settings=None):
        self.settings = settings
        self.failure_classification = {c.id: c.name for c in http.get_json(FAILURE_CLASSIFICATION_URL)}
        self.hg = hg

    @cache
    def get_branches(self):
        response = http.get(self.settings.branches.url, timeout=coalesce(self.settings.timeout, 30))
        branches = convert.json2value(convert.utf82unicode(response.content))
        return wrap({branch.name: unwrap(branch) for branch in branches})

    @cache()
    def get_job_results(self, branch, revision):
        results = http.get_json(expand_template(RESULT_SET_URL, {"branch": branch, "revision": revision[0:12:]}))

        output = []
        for r in results.results:
            jobs = http.get_json(expand_template(JOBS_URL, {"branch": branch, "result_set_id": r.id}))

            for j in jobs.results:
                j.result_set = r
                output.append(j)

        return output

    def _normalize_job_result(self, th_job):
        th_job = wrap(copy(th_job))
        output = Dict()

        output.build.architecture = _scrub(th_job, "build_architecture")
        output.build.os = _scrub(th_job, "build_os")
        output.build.platform = _scrub(th_job, "build_platform")
        output.build.type = _scrub(th_job, "platform_option")

        output.job.id = _scrub(th_job, "id")
        output.job.group.id = _scrub(th_job, "job_group_id")
        output.job.group.name = _scrub(th_job, "job_group_name")
        output.job.group.description = _scrub(th_job, "job_group_description")
        output.job.group.symbol = _scrub(th_job, "job_group_symbol")
        output.job.guid = _scrub(th_job, "job_guid")
        output.job.type.id = _scrub(th_job, "job_type_id")
        output.job.type.description = _scrub(th_job, "job_type_description")
        output.job.type.name = _scrub(th_job, "job_type_name")
        output.job.type.symbol = _scrub(th_job, "job_type_symbol")

        output.run.key = _scrub(th_job, "ref_data_name")

        output.machine.name = _scrub(th_job, "machine_name")
        if Math.is_integer(output.machine.name.split("-")[-1]):
            output.machine.pool = "-".join(output.machine.name.split("-")[:-1])
        output.machine.platform = _scrub(th_job, "machine_platform_architecture")
        output.machine.os = _scrub(th_job, "machine_platform_os")
        output.machine.system = _scrub(th_job, "build_system_type")

        output.job.reason = _scrub(th_job, "reason")
        output.job.state = _scrub(th_job, "state")
        output.job.tier = _scrub(th_job, "tier")
        output.job.who = _scrub(th_job, "who")
        output.job.result = _scrub(th_job, "result")

        output.job.failure_classification = self.failure_classification[_scrub(th_job, "failure_classification_id")]

        output.repo = _scrub(th_job, "result_set")

        output.job.timing.submit = _scrub(th_job, "submit_timestamp")
        output.job.timing.start = _scrub(th_job, "start_timestamp")
        output.job.timing.end = _scrub(th_job, "end_timestamp")
        output.job.timing.last_modified = _scrub(th_job, "last_modified")

        # IGNORED
        th_job.build_platform_id = None
        th_job.job_coalesced_to_guid = None
        th_job.option_collection_hash = None
        th_job.platform = None
        th_job.result_set_id = None
        th_job.running_eta = None
        th_job.signature = None

        if th_job.keys():
            Log.error("{{keys}} are not used", keys=th_job.keys())

        return output

    def get_markup(self, failure):
        """
        GET THE STAR (INTERMITTENT MARKER) AND THE NOTES (RESOLVED BY BUG...)
        """

        # CHECK ES FOR A CACHED VALUE
        job_result = self._get_from_es(failure)
        if job_result:
            return job_result

        # CHECK THERE ARE NO HOLES IN THE JOB IDS






        detail = None
        job_results = self.get_job_results(failure.build.branch, failure.build.revision)
        for job_result in job_results:
            # MATCH TEST RUN BY UID DOES NOT EXIST, SO WE USE THE ARCANE BUILDER NAME
            # PLUS THE MATCHING START/END TIMES

            if job_result.build_system_type == "buildbot" and job_result.ref_data_name != failure.build.name:
                continue
            if job_result.build_system_type == "taskcluster" and job_result.ref_data_name != convert.base642bytes(failure.task.id+"="):
                continue
            # FROM TREEHERDER PERSPECTIVE: THE TEST STARTS BEFORE, AND ENDS AFTER, WHAT ACTIVEDATA SEES
            if job_result.start_timestamp <= failure.run.stats.start_time and failure.run.stats.end_time <= job_result.end_timestamp:
                pass
            else:
                continue

            if detail is not None:
                Log.error("Expecting only one match!")

            detail = self._normalize_job_result(job_result)
            detail.etl = {"timestamp": Date.now()}

            # ATTACH NOTES (RESOLVED BY BUG...)
            notes = http.get_json(expand_template(NOTES_URL, {"branch": failure.build.branch, "job_id": job_result.id}))
            for n in notes:
                n.note = n.note.strip()
                if not n.note:
                    continue

                # LOOK UP REVISION IN REPO
                fix = re.findall(r'[0-9A-Fa-f]{12}', n.note)
                if fix:
                    rev = self.hg.get_revision(Dict(
                        changeset={"id": fix[0]},
                        branch={"name": failure.build.branch}
                    ))
                    n.revision = rev.changeset.id
                    n.bug_id = self.hg._extract_bug_id(rev.changeset.description)

                detail.notes += [{
                    "note": n.note,
                    "revision": n.revision,
                    "bug_id": n.bug_id,
                    "who": n.who,
                    "timestamp": n.timestamp
                }]

            # ATTACH STAR INFO
            stars = http.get_json(expand_template(JOB_BUG_MAP, {"branch": failure.build.branch, "job_id": job_result.id}))
            for s in stars:
                # LOOKUP BUG DETAILS
                detail.stars += [{
                    "bug_id": s.bug_id,
                    "who": s.who,
                    "timestamp": s.submit_timestamp
                }]

        return detail

# https://treeherder.mozilla.org/api/project/mozilla-inbound/resultset/?format=json&full=true&revision=7380457b8ba0
# ** look for "id" at the end
# 2) get jobs list using result_set_id:
# https://treeherder.mozilla.org/api/project/mozilla-inbound/jobs/?count=2000&result_set_id=15367&return_type=list
#
# now look for all jobs that have a status != 'success', and then you can take the jobid for the failing job and get the failure_classification as well as notes:
# https://treeherder.mozilla.org/api/project/mozilla-inbound/note/?job_id=5083103


def _scrub(record, name):
    value = record[name]
    record[name] = None
    if value == "-" or value == "":
        return None
    else:
        return unwraplist(value)

