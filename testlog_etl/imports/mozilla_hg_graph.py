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


from testlog_etl.imports.repos.changesets import Changeset
from testlog_etl.imports.repos.pushs import Push
from testlog_etl.imports.repos.revisions import Revision
from pyLibrary import convert
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.dot import coalesce
from pyLibrary.dot import unwrap, wrap
from pyLibrary.env import http
from pyLibrary.maths import Math
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration


class MozillaHgGraph(object):
    """
    VERY SLOW, PURE hg.moziila.org GRAPH IMPLEMENTATION
    """

    def __init__(self, settings):
        self.settings = wrap(settings)
        self.settings.timeout = Duration(coalesce(self.settings.timeout, "30second"))
        self.nodes = {}  # DUMB CACHE FROM (branch, changeset_id) TO REVISOIN
        self.pushes = {}  # MAP FROM (branch, changeset_id) TO Push

    def get_node(self, revision):
        """
        EXPECTING INCOMPLETE revision
        RETURNS revision
        """
        if len(revision.changeset.id) < 12 and Math.is_integer(revision.changeset.id):
            revision.changeset.id = ("0" * (12 - len(revision.changeset.id))) + revision.changeset.id

        revision.branch = self.settings.branches[revision.branch.name.lower()]
        if revision in self.nodes:
            output = self.nodes[revision]
            if isinstance(output, Except):
                raise output  # WE STORE THIS EXCEPTION SO WE DO NOT TRY TO GET REVISION INFO TOO MANY TIMES
            else:
                return output

        url = revision.branch.url + "/json-info?node=" + revision.changeset.id
        try:
            Log.note("Reading details for from {{url}}", {"url": url})

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
                    date=Date(r.date).unix
                ),
                parents=r.parents,
                children=r.children,
                files=r.files,
                graph=self
            )
            self.nodes[revision]=revision
            return output
        except Exception, e:
            try:
                Log.error("Can not get revision info from {{url}}", {"url": url}, e)
            except Exception, e:
                self.nodes[revision] = e  # WE STORE THIS EXCEPTION SO WE DO NOT TRY TO GET REVISION INFO TOO MANY TIMES
                raise e

    def get_push(self, revision):
        # http://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=57c461500a0c
        if revision not in self.pushes:
            Log.note(
                "Reading pushlog for revision ({{branch}}, {{changeset}})",
                branch=revision.branch.name,
                changeset=revision.changeset.id
            )

            url = revision.branch.url + "/json-pushes?full=1&changeset=" + revision.changeset.id
            try:
                response = self._get_and_retry(url)
                data = convert.json2value(response.content.decode("utf8"))
                for index, _push in data.items():
                    push = Push(index, revision.branch, _push.date, _push.user)
                    for c in _push.changesets:
                        changeset = Changeset(id=c.node, **unwrap(c))
                        rev = Revision(branch=revision.branch, changeset=changeset, graph=self)
                        self.pushes[rev] = push
                        push.changesets.append(changeset)
            except Exception, e:
                Log.error("Problem pulling pushlog from {{url}}", {"url": url}, e)

        push = self.pushes[revision]
        revision.push = push
        return push

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
        revision = self.get_node(revision)
        if not revision[subset]:
            return []
        elif len(revision[subset]) == 1:
            return [self.get_node(Revision(branch=revision.branch, changeset=c, graph=self)) for c in revision[subset]]
        else:
            #MULTIPLE BRANCHES ARE A HINT OF A MERGE BETWEEN BRANCHES
            output = []
            for branch in self.settings.branches.values():
                for c in revision[subset]:
                    node = self.get_node(Revision(branch=branch, changeset=c, graph=self))
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
