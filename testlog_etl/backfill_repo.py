# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, unwraplist, unwrap, wrap, wrap_dot
from pyLibrary.env import elasticsearch
from pyLibrary.queries.index import Index
from pyLibrary.thread.threads import Thread, Signal
from testlog_etl.imports.hg_mozilla_org import HgMozillaOrg


def worker(settings, please_stop):
    hg = HgMozillaOrg(settings)

    detailed = Index(None, keys=("changeset.id", "branch.name", "branch.locale"))
    known = Index(None, keys=("changeset.id", "branch.name", "branch.locale"))
    frontier = Index(None, keys=("changeset.id", "branch.name", "branch.locale"))

    # INTERCEPT THE NEW CHANGESETS DESTINED FOR ES
    old_extend = hg.es.extend

    def extend(inserts):
        docs = wrap(inserts).value
        for d in docs:
            if len(d.parents) == 1:
                for p in d.parents:
                    known[p] = d.branch.name
                    frontier.add(p)
            else:
                for p in d.parents:
                    known[p] = None
                    frontier.add(p)
        for d in docs:
            detailed[d.changeset.id] = d.branch.name
            frontier.discard(d.changeset.id)
        old_extend(inserts)
    setattr(hg.es, "extend", extend)

    # FIND THE FRONTIER
    query = {
        "query": {"match_all": {}},
        "fields": ["branch.name", "branch.locale", "changeset.id", "parents"],
        "size": 200000,
    }
    docs = hg.es.search(query).hits.hits
    for d in unwrap(docs):
        r = elasticsearch.scrub(wrap_dot(d["fields"]))
        detailed.add(r)
        parents = listwrap(r.parents)
        if len(parents) == 1:
            for p in parents:
                known.add({"branch": {"name": r.branch.name, "locale": r.branch.name}, "changeset": {"id": p}})
        else:
            for p in parents:
                known.add({"changeset": {"id": p}})

    def getall():
        branches = hg.find_changeset(r.changeset.id)
        for b in branches:
            hg.get_revision(wrap({"changeset": {"id": r}, "branch": {"name": b}}))


    frontier |= known - detailed
    while not please_stop and frontier:
        r = frontier.pop()
        if r.branch == None:
            getall()
        else:
            try:
                rev = hg.get_revision(r)
            except Exception, e:
                getall()


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        stopper = Signal()
        Thread.run("backfill repo", worker, settings.hg, please_stop=stopper)
        Thread.wait_for_shutdown_signal(stopper, allow_exit=True)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

