import re

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap, unwrap, Dict
from pyLibrary.env import http
from pyLibrary.meta import cache, use_settings
from pyLibrary.strings import expand_template
from pyLibrary.times.dates import Date
from mohg.hg_mozilla_org import HgMozillaOrg

RESULT_SET_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/resultset/?format=json&full=true&revision__in={{revision}}"
JOBS_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/jobs/?count=2000&result_set_id__in={{result_set_id}}&failure_classification_id__ne=1"
NOTES_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/note/?job_id={{job_id}}"
JOB_BUG_MAP = "https://treeherder.mozilla.org/api/project/{{branch}}/bug-job-map/?job_id={{job_id}}"
FAILURE_CLASSIFICATION_URL = "https://treeherder.mozilla.org/api/failureclassification/"


class TreeHerder(object):
    @use_settings
    def __init__(self, repo=None, branches=None, timeout=None, use_cache=True, settings=None):
        self.settings = settings
        self.failure_classification = {c.id: c.name for c in http.get_json(FAILURE_CLASSIFICATION_URL)}
        self.hg = HgMozillaOrg(repo=repo, branches=branches, use_cache=use_cache)

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

    def get_markup(self, failure):
        """
        GET THE STAR (INTERMITTENT MARKER) AND THE NOTES (RESOLVED BY BUG...)
        """
        detail = None
        job_results = self.get_job_results(failure.build.branch, failure.build.revision)
        for job_result in job_results:
            # MATCH TEST RUN BY UID DOES NOT EXIST, SO WE USE THE ARCANE BUILDER NAME
            # PLUS THE MATCHING START/END TIMES
            if job_result.ref_data_name != failure.build.name:
                continue
            # FROM TREEHERDER PERSPECTIVE: THE TEST STARTS BEFORE, AND ENDS AFTER, WHAT ACTIVEDATA SEES
            if job_result.start_timestamp <= failure.run.stats.start_time and failure.run.stats.end_time <= job_result.end_timestamp:
                pass
            else:
                continue

            if detail is not None:
                Log.error("Expecting only one match!")

            detail = wrap({
                "job_id": job_result.id,
                "result_set_id": job_result.result_set.id,
                "failure_classification": self.failure_classification[job_result.failure_classification_id],
                "result": job_result.result,
                "etl": {"timestamp": Date.now()}
            })

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
