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
from copy import copy
import re

from pyLibrary.debugs.exceptions import suppress_exception, Explanation, assert_no_exception
from pyLibrary.meta import use_settings, cache
from pyLibrary.queries import jx
from pyLibrary.testing import elasticsearch
from pyLibrary import convert, strings
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import set_default, Null, coalesce, unwraplist
from pyLibrary.env import http
from pyLibrary.thread.threads import Thread, Lock, Queue
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import SECOND, Duration, HOUR, DAY
from mohg.repos.changesets import Changeset
from mohg.repos.pushs import Push
from mohg.repos.revisions import Revision


_hg_branches = None
_OLD_BRANCH = None


def _late_imports():
    global _hg_branches
    global _OLD_BRANCH

    from mohg import hg_branches as _hg_branches
    from mohg.hg_branches import OLD_BRANCH as _OLD_BRANCH

    _ = _hg_branches
    _ = _OLD_BRANCH


DEFAULT_LOCALE = "en-US"
DEBUG = False

class HgMozillaOrg(object):
    """
    USE hg.mozilla.org FOR REPO INFORMATION
    USE ES AS A FASTER CACHE FOR THE SAME
    """

    @use_settings
    def __init__(
        self,
        hg=None,        # CONNECT TO hg
        repo=None,      # CONNECTION INFO FOR ES CACHE
        branches=None,  # CONNECTION INFO FOR ES CACHE
        use_cache=False,   # True IF WE WILL USE THE ES FOR DOWNLOADING BRANCHES
        timeout=30 * SECOND,
        settings=None
    ):
        if not _hg_branches:
            _late_imports()

        self.settings = settings
        self.timeout = Duration(timeout)

        if branches == None:
            self.branches = _hg_branches.get_branches(settings=settings)
            self.es = None
            return

        self.es = elasticsearch.Cluster(settings=repo).get_or_create_index(settings=repo)
        self.es.add_alias()
        self.es.set_refresh_interval(seconds=1)

        self.branches = _hg_branches.get_branches(use_cache=use_cache, settings=settings)

        # TO ESTABLISH DATA
        self.es.add({"id": "b3649fd5cd7a-mozilla-inbound-en-US", "value": {
            "index": 247152,
            "branch": {
                "name": "mozilla-inbound",
                "locale": DEFAULT_LOCALE
            },
            "changeset": {
                "id": "b3649fd5cd7a76506d2cf04f45e39cbc972fb553",
                "id12": "b3649fd5cd7a",
                "author": "ryan",
                "description": "Backed out changeset 7d0d8d304cd8 (bug 1171357) for bustage.",
                "date": 1433429100,
                "files": ["gfx/thebes/gfxTextRun.cpp"]
            },
            "push": {
                "id": 60618,
                "user": "ryan",
                "date": 1433429138
            },
            "parents": ["7d0d8d304cd871f657effcc2d21d4eae5155fd1b"],
            "children": ["411a9af141781c3c8fa883287966a4af348dbca8"]
        }})
        self.es.flush()

    @cache(duration=HOUR, lock=True)
    def get_revision(self, revision, locale=None):
        """
        EXPECTING INCOMPLETE revision
        RETURNS revision
        """
        rev = revision.changeset.id
        if not rev:
            return Null
        elif rev == "None":
            return Null
        elif revision.branch.name == None:
            return Null
        locale = coalesce(locale, revision.branch.locale, DEFAULT_LOCALE)
        doc = self._get_from_elasticsearch(revision, locale=locale)
        if doc:
            Log.note("Got hg ({{branch}}, {{locale}}, {{revision}}) from ES", branch=doc.branch.name, locale=locale, revision=doc.changeset.id)
            return doc

        output = self._load_all_in_push(revision, locale=locale)
        return output

    def _get_from_elasticsearch(self, revision, locale=None):
        rev = revision.changeset.id
        query = {
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and": [
                    {"prefix": {"changeset.id": rev[0:12]}},
                    {"term": {"branch.name": revision.branch.name}},
                    {"term": {"branch.locale": coalesce(locale, revision.branch.locale, DEFAULT_LOCALE)}}
                ]}
            }},
            "size": 2000,
        }
        try:
            docs = self.es.search(query, timeout=120).hits.hits
            if len(docs) > 1:
                for d in docs:
                    if d._id.endswith(d._source.branch.locale):
                        return d._source
                Log.warning("expecting no more than one document")

            return docs[0]._source
        except Exception, e:
            Log.warning("Bad ES call, fall back to hg", e)
            return None

    def _load_all_in_push(self, revision, locale=None):
        # http://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=57c461500a0c
        found_revision = copy(revision)
        if isinstance(found_revision.branch, basestring):
            lower_name = found_revision.branch.lower()
        else:
            lower_name = found_revision.branch.name.lower()

        if not lower_name:
            Log.error("Defective revision? {{rev|json}}", rev=found_revision.branch)

        b = found_revision.branch = self.branches[(lower_name, locale)]
        if not b:
            b = found_revision.branch = self.branches[(lower_name, DEFAULT_LOCALE)]
            if not b:
                Log.error("can not find branch ({{branch}}, {{locale}})", name=lower_name, locale=locale)
        if Date.now() - Date(b.etl.timestamp) > _OLD_BRANCH:
            self.branches = _hg_branches.get_branches(use_cache=True, settings=self.settings)

        url = found_revision.branch.url.rstrip("/") + "/json-pushes?full=1&changeset=" + found_revision.changeset.id
        Log.note(
            "Reading pushlog for revision ({{branch}}, {{locale}}, {{changeset}}): {{url}}",
            branch=found_revision.branch.name,
            locale=locale,
            changeset=found_revision.changeset.id,
            url=url
        )

        with Explanation("Pulling pushlog from {{url}}", url=url):
            data = self._get_and_retry(url, found_revision.branch)

            revs = []
            output = None
            for index, _push in data.items():
                push = Push(id=int(index), date=_push.date, user=_push.user)

                for _, ids in jx.groupby(_push.changesets.node, size=200):
                    url_param = "&".join("node=" + c[0:12] for c in ids)

                    url = found_revision.branch.url.rstrip("/") + "/json-info?" + url_param
                    Log.note("Reading details from {{url}}", {"url": url})

                    raw_revs = self._get_and_retry(url, found_revision.branch)
                    for r in raw_revs.values():
                        rev = Revision(
                            branch=found_revision.branch,
                            index=r.rev,
                            changeset=Changeset(
                                id=r.node,
                                id12=r.node[0:12],
                                author=r.user,
                                description=r.description,
                                date=Date(r.date),
                                files=r.files,
                                backedoutby=r.backedoutby
                            ),
                            parents=unwraplist(r.parents),
                            children=unwraplist(r.children),
                            push=push,
                            etl={"timestamp": Date.now().unix}
                        )
                        if r.node == found_revision.changeset.id:
                            output = rev
                        if r.node[0:12] == found_revision.changeset.id[0:12]:
                            output = rev
                        _id = coalesce(rev.changeset.id12, "") + "-" + rev.branch.name + "-" + coalesce(rev.branch.locale, DEFAULT_LOCALE)
                        revs.append({"id": _id, "value": rev})
            self.es.extend(revs)
            return output

    def _get_and_retry(self, url, branch, **kwargs):
        """
        requests 2.5.0 HTTPS IS A LITTLE UNSTABLE
        """
        kwargs = set_default(kwargs, {"timeout": self.timeout.seconds})
        try:
            return _get_url(url, branch, **kwargs)
        except Exception, e:
            pass

        try:
            Thread.sleep(seconds=5)
            return _get_url(url.replace("https://", "http://"), branch, **kwargs)
        except Exception, f:
            pass

        path = url.split("/")
        if path[3] == "l10n-central":
            # FROM https://hg.mozilla.org/l10n-central/tr/json-pushes?full=1&changeset=a6eeb28458fd
            # TO   https://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=a6eeb28458fd
            path = path[0:3] + ["mozilla-central"] + path[5:]
            return self._get_and_retry("/".join(path), branch, **kwargs)
        elif len(path) > 5 and path[5] == "mozilla-aurora":
            # FROM https://hg.mozilla.org/releases/l10n/mozilla-aurora/pt-PT/json-pushes?full=1&changeset=b44a8c68fc60
            # TO   https://hg.mozilla.org/releases/mozilla-aurora/json-pushes?full=1&changeset=b44a8c68fc60
            path = path[0:4] + ["mozilla-aurora"] + path[7:]
            return self._get_and_retry("/".join(path), branch, **kwargs)
        elif len(path) > 5 and path[5] == "mozilla-beta":
            # FROM https://hg.mozilla.org/releases/l10n/mozilla-beta/lt/json-pushes?full=1&changeset=03fbf7556c94
            # TO   https://hg.mozilla.org/releases/mozilla-beta/json-pushes?full=1&changeset=b44a8c68fc60
            path = path[0:4] + ["mozilla-beta"] + path[7:]
            return self._get_and_retry("/".join(path), branch, **kwargs)
        elif len(path) > 7 and path[5] == "mozilla-release":
            # FROM http://hg.mozilla.org/releases/l10n/mozilla-release/en-GB/json-pushes?full=1&changeset=57f513ab03308adc7aa02cc2ea8d73fe56ae644b
            # TO   https://hg.mozilla.org/releases/mozilla-release/json-pushes?full=1&changeset=57f513ab03308adc7aa02cc2ea8d73fe56ae644b
            path = path[0:4] + ["mozilla-release"] + path[7:]
            return self._get_and_retry("/".join(path), branch, **kwargs)
        elif len(path) > 5 and path[4] == "autoland":
            # FROM https://hg.mozilla.org/build/autoland/json-pushes?full=1&changeset=3ccccf8e5036179a3178437cabc154b5e04b333d
            # TO  https://hg.mozilla.org/integration/autoland/json-pushes?full=1&changeset=3ccccf8e5036179a3178437cabc154b5e04b333d
            path = path[0:3] + ["try"] + path[5:]
            return self._get_and_retry("/".join(path), branch, **kwargs)

        Log.error("Tried {{url}} twice.  Both failed.", {"url": url}, cause=[e, f])

    @cache(duration=HOUR, lock=True)
    def find_changeset(self, revision, please_stop=False):
        locker = Lock()
        output = []
        queue = Queue("branches", max=2000)
        queue.extend(self.branches)
        queue.add(Thread.STOP)

        problems = []
        def _find(please_stop):
            for b in queue:
                if please_stop:
                    return
                try:
                    url = b.url + "json-info?node=" + revision
                    response = http.get(url, timeout=30)
                    if response.status_code == 200:
                        with locker:
                            output.append(b)
                        Log.note("{{revision}} found at {{url}}", url=url, revision=revision)
                except Exception, f:
                    problems.append(f)

        threads = []
        for i in range(20):
            threads.append(Thread.run("find changeset " + unicode(i), _find, please_stop=please_stop))

        for t in threads:
            with assert_no_exception:
                t.join()

        if problems:
            Log.error("Could not scan for {{revision}}", revision=revision, cause=problems[0])

        return output

    def _extract_bug_id(self, description):
        """
        LOOK INTO description to FIND bug_id
        """
        match = re.match(r'[Bb](ug)?\s*([0-9]{5,7})\s+', description)
        if match:
            return int(match.group(2))
        return None



def _trim(url):
    return url.split("/json-pushes?")[0].split("/json-info?")[0]


def _get_url(url, branch, **kwargs):
    with Explanation("get push from {{url}}", url=url):
        response = http.get(url, **kwargs)
        data = convert.json2value(response.content.decode("utf8"))
        if isinstance(data, basestring) and data.startswith("unknown revision"):
            Log.error("Unknown push {{revision}}", revision=strings.between(data, "'", "'"))
        branch.url = _trim(url)  #RECORD THIS SUCCESS IN THE BRANCH
        return data
