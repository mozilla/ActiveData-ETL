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
from pyLibrary.meta import use_settings, cache

from testlog_etl.imports.repos.changesets import Changeset
from testlog_etl.imports.repos.pushs import Push
from testlog_etl.imports.repos.revisions import Revision
from pyLibrary import convert, strings
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.dot import coalesce
from pyLibrary.dot import unwrap, wrap
from pyLibrary.env import http
from pyLibrary.maths import Math
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration, DAY, MINUTE, SECOND


class MozillaHgGraph(object):
    """
    VERY SLOW, PURE hg.moziila.org GRAPH IMPLEMENTATION
    """

    @use_settings
    def __init__(
        self,
        branches,
        cache_duration=DAY,
        timeout=30 * SECOND,
        settings=None
    ):
        self.settings = settings
        self.current_push = None


    @cache(duration=DAY)
    def get_revision(self, revision):
        """
        EXPECTING INCOMPLETE revision
        RETURNS revision
        """
        if not self.current_push:
            self._load_all_in_push(revision)
            # THE cache IS FILLED, CALL ONE LAST TIME...
            return self.get_revision(revision)

        if len(revision.changeset.id) < 12 and Math.is_integer(revision.changeset.id):
            revision.changeset.id = ("0" * (12 - len(revision.changeset.id))) + revision.changeset.id

        revision.branch = self.settings.branches[revision.branch.name.lower()]

        url = revision.branch.url + "/json-info?node=" + revision.changeset.id
        try:
            Log.note("Reading details for from {{url}}", {"url": url})

            response = self._get_and_retry(url)
            revs = convert.json2value(response.all_content.decode("utf8"))

            if revs.startswith("unknown revision "):
                Log.error(revs)

            if len(revs.keys()) != 1:
                Log.error("Do not know how to handle")

            r = list(revs.values())[0]
            output = Revision(
                branch=revision.branch.name,
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
            )
            return output
        except Exception, e:
            Log.error("Can not get revision info from {{url}}", {"url": url}, e)

    def _load_all_in_push(self, revision):
        # http://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=57c461500a0c

        if isinstance(revision.branch, basestring):
            revision.branch = self.settings.branches[revision.branch]
        else:
            revision.branch = self.settings.branches[revision.branch.name.lower()]

        Log.note(
            "Reading pushlog for revision ({{branch}}, {{changeset}})",
            branch=revision.branch.name,
            changeset=revision.changeset.id
        )

        url = revision.branch.url + "/json-pushes?full=1&changeset=" + revision.changeset.id
        try:
            response = self._get_and_retry(url)
            data = convert.json2value(response.all_content.decode("utf8"))
            if isinstance(data, basestring) and data.startswith("unknown revision"):
                Log.error("Unknown revision {{revision}}", revision=strings.between(data, "'", "'"))
            for index, _push in data.items():
                push = Push(id=index, date=_push.date, user=_push.user)
                self.current_push = push
                for c in _push.changesets:
                    changeset = Changeset(id=c.node, **c)
                    rev = self.get_revision(Revision(branch=revision.branch, changeset=changeset))
                    rev.push = push
        except Exception, e:
            Log.error("Problem pulling pushlog from {{url}}", url=url, cause=e)
        finally:
            self.current_push = None

    def get_edges(self, revision):
        output = []
        for c in self.get_children(revision):
            output.append((revision, c))
        for p in self.get_parents(revision):
            output.append((p, revision))
        return output

    def get_family(self, revision):
        return set(self.get_children(revision) + self.get_parents(revision))

    def get_children(self, revision):
        return self._get_adjacent(revision, "children")

    def get_parents(self, revision):
        return self._get_adjacent(revision, "parents")

    def _get_adjacent(self, revision, subset):
        revision = self.get_revision(revision)
        if not revision[subset]:
            return []
        elif len(revision[subset]) == 1:
            return [self.get_revision(Revision(branch=revision.branch, changeset=c)) for c in revision[subset]]
        else:
            # MULTIPLE BRANCHES ARE A HINT OF A MERGE BETWEEN BRANCHES
            output = []
            for branch in self.settings.branches.values():
                for c in revision[subset]:
                    node = self.get_revision(Revision(branch=branch, changeset=c))
                    if node:
                        output.append(node)
            return output

    def _get_and_retry(self, url, **kwargs):
        """
        requests 2.5.0 HTTPS IS A LITTLE UNSTABLE
        """
        kwargs = wrap(kwargs)
        kwargs.setdefault("timeout", self.settings.timeout.seconds)
        try:
            return http.get(url, **unwrap(kwargs))
        except Exception, e:
            try:
                Thread.sleep(seconds=5)
                return http.get(url.replace("https://", "http://"), **unwrap(kwargs))
            except Exception, f:
                Log.error("Tried {{url}} twice.  Both failed.", {"url": url}, [e, f])
