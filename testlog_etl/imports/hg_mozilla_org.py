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
from pyLibrary.meta import use_settings, cache
from pyLibrary.queries import qb
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.testing import elasticsearch

from testlog_etl.imports.repos.changesets import Changeset
from testlog_etl.imports.repos.pushs import Push
from testlog_etl.imports.repos.revisions import Revision
from pyLibrary import convert, strings
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import set_default, Null, coalesce, listwrap, unwraplist
from pyLibrary.env import http
from pyLibrary.maths import Math
from pyLibrary.thread.threads import Thread, Lock, Queue
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY, SECOND, Duration, HOUR


DEFAULT_LOCALE = "en-US"
DEBUG = True

class HgMozillaOrg(object):
    """
    USE hg.mozilla.org FOR REPO INFORMATION
    USE ES AS A FASTER CACHE FOR THE SAME
    """

    @use_settings
    def __init__(
        self,
        repo=None,      # CONNECTION INFO FOR ES CACHE
        branches=None,  # CONNECTION INFO FOR ES CACHE
        use_cache=False,   # True IF WE WILL USE THE ES FOR DOWNLOADING BRANCHES
        timeout=30 * SECOND,
        settings=None
    ):
        self.settings = settings
        self.timeout = Duration(timeout)
        self.current_push = None

        if branches == None:
            self.branches = self.get_branches()
            self.es = None
            return

        self.es = elasticsearch.Cluster(settings=repo).get_or_create_index(settings=repo)
        self.es.add_alias()
        self.es.set_refresh_interval(seconds=1)

        self.branches = self.get_branches(use_cache=use_cache)

        # TO ESTABLISH DATA
        self.es.add({"id": "b3649fd5cd7a-mozilla-inbound", "value": {
            "index": 247152,
            "branch": {
                "name": "mozilla-inbound",
                "locale": DEFAULT_LOCALE
            },
            "changeset": {
                "id": "b3649fd5cd7a76506d2cf04f45e39cbc972fb553",
                "id12": "b3649fd5cd7a",
                "author": "Ryan VanderMeulen <ryanvm@gmail.com>",
                "description": "Backed out changeset 7d0d8d304cd8 (bug 1171357) for bustage.",
                "date": 1433429100,
                "files": ["gfx/thebes/gfxTextRun.cpp"]
            },
            "push": {
                "id": 60618,
                "user": "ryanvm@gmail.com",
                "date": 1433429138
            },
            "parents": ["7d0d8d304cd871f657effcc2d21d4eae5155fd1b"],
            "children": ["411a9af141781c3c8fa883287966a4af348dbca8"]
        }})
        self.es.flush()

    @cache(duration=DAY, lock=True)
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
        if not self.current_push:
            doc = self._get_from_elasticsearch(revision, locale=locale)
            if doc:
                Log.note("Got hg ({{branch}}, {{locale}}, {{revision}}) from ES", branch=doc.branch.name, locale=locale, revision=doc.changeset.id)
                return doc

            num_in_push = self._load_all_in_push(revision, locale=locale)

            # THE cache IS FILLED, CALL ONE LAST TIME...
            if num_in_push == 0:
                return None
            else:
                return self.get_revision(revision, locale)

        output = self._get_from_hg(revision, locale=locale)
        output.changeset.id12 = output.changeset.id[0:12]
        output.branch = {
            "name": output.branch.name,
            "url": output.branch.url,
            "locale": output.branch.locale
        }
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
        docs = self.es.search(query).hits.hits
        if len(docs) > 1:
            Log.error("expecting no more than one document")

        return docs[0]._source

    def _get_from_hg(self, revision, locale=None):
        rev = revision.changeset.id
        if len(rev) < 12 and Math.is_integer(rev):
            rev = ("0" * (12 - len(rev))) + rev

        revision.branch = self.branches[revision.branch.name.lower(), coalesce(locale, revision.branch.locale, DEFAULT_LOCALE)]

        url = revision.branch.url.rstrip("/") + "/json-info?node=" + rev
        try:
            Log.note("Reading details from {{url}}", {"url": url})

            response = self._get_and_retry(url)
            revs = convert.json2value(response.content.decode("utf8"))

            if revs.startswith("unknown revision "):
                Log.error(revs)

            if len(revs.keys()) != 1:
                Log.error("Do not know how to handle")

            r = list(revs.values())[0]
            output = Revision(
                branch=revision.branch,
                index=r.rev,
                changeset=Changeset(
                    id=r.node,
                    author=r.user,
                    description=r.description,
                    date=Date(r.date),
                    files=r.files
                ),
                parents=r.parents,
                children=r.children,
                etl={"timestamp": Date.now().unix}
            )
            return output
        except Exception, e:
            Log.error("Can not get revision info from {{url}}", {"url": url}, e)

    def _load_all_in_push(self, revision, locale=None):
        # http://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=57c461500a0c
        found_revision = copy(revision)
        if isinstance(found_revision.branch, basestring):
            lower_name = found_revision.branch.lower()
        else:
            lower_name = found_revision.branch.name.lower()

        if not lower_name:
            Log.error("Defective revision? {{rev|json}}", rev=found_revision.branch)

        found_revision.branch = self.branches[(lower_name, locale)]
        if not found_revision.branch:
            Log.error("can not find branch ({{branch}}, {{locale}})", name=lower_name, locale=locale)

        url = found_revision.branch.url.rstrip("/") + "/json-pushes?full=1&changeset=" + found_revision.changeset.id
        Log.note(
            "Reading pushlog for revision ({{branch}}, {{locale}}, {{changeset}}): {{url}}",
            branch=found_revision.branch.name,
            locale=locale,
            changeset=found_revision.changeset.id,
            url=url
        )

        try:
            response = self._get_and_retry(url)
            data = convert.json2value(response.content.decode("utf8"))
            if isinstance(data, basestring) and data.startswith("unknown revision"):
                Log.error("Unknown push {{revision}}", revision=strings.between(data, "'", "'"))

            revs = []
            for index, _push in data.items():
                push = Push(id=int(index), date=_push.date, user=_push.user)
                self.current_push = push

                for _, ids in qb.groupby(_push.changesets.node, size=200):
                    url_param = "&".join("node=" + c[0:12] for c in ids)

                    url = found_revision.branch.url.rstrip("/") + "/json-info?" + url_param
                    Log.note("Reading details from {{url}}", {"url": url})

                    response = self._get_and_retry(url)
                    raw_revs = convert.json2value(response.content.decode("utf8"))
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
                                files=r.files
                            ),
                            parents=unwraplist(r.parents),
                            children=unwraplist(r.children),
                            push=push,
                            etl={"timestamp": Date.now().unix}
                        )
                        _id = coalesce(rev.changeset.id12, "") + "-" + rev.branch.name + "-" + coalesce(rev.branch.locale, DEFAULT_LOCALE)
                        revs.append({"id": _id, "value": rev})
            num = len(revs)
            self.es.extend(revs)
            return num
        except Exception, e:
            Log.error("Problem pulling pushlog from {{url}}", url=url, cause=e)
        finally:
            self.current_push = None

    def _get_and_retry(self, url, **kwargs):
        """
        requests 2.5.0 HTTPS IS A LITTLE UNSTABLE
        """
        kwargs = set_default(kwargs, {"timeout": self.timeout.seconds})
        try:
            return http.get(url, **kwargs)
        except Exception, e:
            try:
                Thread.sleep(seconds=5)
                return http.get(url.replace("https://", "http://"), **kwargs)
            except Exception, f:
                Log.error("Tried {{url}} twice.  Both failed.", {"url": url}, cause=[e, f])

    def get_branches(self, use_cache=True):
        if not self.settings.branches or not use_cache:
            from testlog_etl import etl_hg_branch

            return etl_hg_branch.get_branches(settings={"url": "https://hg.mozilla.org"})

        #TRY ES
        es = elasticsearch.Cluster(settings=self.settings.branches).get_index(settings=self.settings.branches)
        query = {
            "query": {"match_all": {}},
            "size": 20000
        }

        docs = es.search(query).hits.hits._source
        for d in docs:
            d.name = d.name.lower()
        try:
            return UniqueIndex(["name", "locale"], data=docs, fail_on_dup=False)
        except Exception, e:
            Log.error("Bad branch in ES index", cause=e)

    @cache(duration=HOUR, lock=True)
    def find_changeset(self, revision):
        locker = Lock()
        output = []
        queue = Queue("branches", max=2000)
        queue.extend(self.branches)
        queue.add(Thread.STOP)

        def _find(please_stop):
            for b in queue:
                try:
                    url = b.url + "rev/" + revision
                    response = http.get(url)
                    if response.status_code == 200:
                        with locker:
                            output.append(b)
                        Log.note("{{revision}} found at {{url}}", url=url, revision=revision)
                except Exception, e:
                    pass

        threads = []
        for _ in range(20):
            threads.append(Thread.run("find changeset", _find))

        for t in threads:
            t.join()
        return output

    def _extract_bug_id(self, description):
        """
        LOOK INTO description to FIND bug_id
        """
        match = re.match(r'[Bb](ug)?\s*([0-9]{5,7})\s+', description)
        if match:
            return int(match.group(2))
        return None


