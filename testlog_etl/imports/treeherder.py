
from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap, unwrap
from pyLibrary.env import http
from pyLibrary.meta import cache, use_settings
from pyLibrary.strings import expand_template

RESULT_SET_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/resultset/?format=json&full=true&revision={{revision}}"
JOBS_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/jobs/?count=2000&result_set_id={{result_set_id}}&failure_classification_id__ne=1"
NOTES_URL = "https://treeherder.mozilla.org/api/project/{{branch}}/note/?job_id={{job_id}}"

class TreeHerder(object):
    @use_settings
    def __init__(self, timeout=None, settings=None):
        self.settings = settings

    @cache
    def get_branches(self):
        response = http.get(self.settings.branches.url, timeout=coalesce(self.settings.timeout, 30))
        branches = convert.json2value(convert.utf82unicode(response.all_content))
        return wrap({branch.name: unwrap(branch) for branch in branches})


    def get_job_classification(self, branch, revision):
        results = http.get_json(expand_template(RESULT_SET_URL, {"branch": branch, "revision": revision[0:12:]}))
        for r in results.results:
            jobs = http.get_json(expand_template(JOBS_URL, {"branch": branch, "result_set_id": r.id}))
            for j in jobs.results:
                notes = http.get_json(expand_template(NOTES_URL, {"branch": branch, "job_id": j.id}))
                for n in notes:
                    if not n.note:
                        continue

                    Log.note(
                        "{{note|json}}",
                        note={
                            "branch": branch,
                            "revision": r.revision,
                            "failure_classification_id": j.failure_classification_id,
                            "result": j.result,
                            "note_timestamp": n.timestamp,
                            "note": n.note
                        }
                    )

# https://treeherder.mozilla.org/api/project/mozilla-inbound/resultset/?format=json&full=true&revision=7380457b8ba0
# ** look for "id" at the end
# 2) get jobs list using result_set_id:
# https://treeherder.mozilla.org/api/project/mozilla-inbound/jobs/?count=2000&result_set_id=15367&return_type=list
#
# now look for all jobs that have a status != 'success', and then you can take the jobid for the failing job and get the failure_classification as well as notes:
# https://treeherder.mozilla.org/api/project/mozilla-inbound/note/?job_id=5083103
