# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary.collections.queue import Queue

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, unwrap, wrap, wrap_dot
from pyLibrary.env import elasticsearch
from pyLibrary.maths import Math
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.thread.threads import Thread, Signal
from pyLibrary.times.dates import Date
from testlog_etl.imports.hg_mozilla_org import HgMozillaOrg, DEFAULT_LOCALE


DEBUG = True
MIN_DATE=Date("01 MAR 2015")

current_revision = None

def get_frontier(hg):
    # FIND THE FRONTIER
    detailed = UniqueIndex(keys=("changeset.id", "branch.name", "branch.locale"), fail_on_dup=False)
    known = UniqueIndex(keys=("changeset.id", "branch.name", "branch.locale"), fail_on_dup=False)
    query = {
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": {"and": [
                {"exists": {"field": "branch.name"}},
                {"exists": {"field": "branch.locale"}},
                {"range": {"changeset.date": {"gte": MIN_DATE}}}
            ]}
        }},
        "fields": ["branch.name", "branch.locale", "changeset.id", "parents"],
        "size": 100 if DEBUG else 200000,
    }
    docs = hg.es.search(query).hits.hits
    for d in unwrap(docs):
        r = elasticsearch.scrub(wrap_dot(d["fields"]))
        detailed.add(r)
        parents = listwrap(r.parents)
        if len(parents) == 1:
            for p in parents:
                known.add({"branch": r.branch, "changeset": {"id": p}})
        else:
            for p in parents:
                known.add({"changeset": {"id": p}})


    return known - detailed


def patch_es(es, frontier):
    # INTERCEPT THE NEW CHANGESETS DESTINED FOR ES
    global current_revision
    old_extend = es.extend

    def extend(inserts):
        docs = wrap(inserts).value
        for d in docs:
            if d.changeset.date < MIN_DATE:
                continue  # DO NOT FOLLOW OLD PATHS

            parents = listwrap(d.parents)
            if len(parents) == 1:
                for p in parents:
                    frontier.add({"branch": current_revision.branch, "changeset": {"id": p}})
            else:
                for p in parents:
                    frontier.add({"changeset": {"id": p}})
        for d in docs:
            frontier.remove(d)

        old_extend(inserts)

    es.set_refresh_interval(seconds=1)
    setattr(es, "extend", extend)

def getall(hg, please_stop):
    global current_revision
    branches = hg.find_changeset(current_revision.changeset.id, please_stop)
    for b in branches:
        if please_stop:
            Log.error("Exit early")
        hg.get_revision(wrap({"changeset": {"id": current_revision.changeset.id}, "branch": b}))


def worker(settings, please_stop):
    global current_revision
    hg = HgMozillaOrg(settings)

    frontier = UniqueIndex(keys=("changeset.id", "branch.name", "branch.locale"), fail_on_dup=False)
    frontier |= get_frontier(hg)

    patch_es(hg.es, frontier)

    try:
        while not please_stop and frontier:
            current_revision = frontier.pop()
            if not current_revision.branch:
                getall(hg, please_stop)
                continue

            if not current_revision.branch.locale:
                current_revision.branch.locale = DEFAULT_LOCALE
            try:
                rev = hg.get_revision(current_revision)
                frontier.remove(rev)
            except Exception, e:
                Log.warning("can not get {{rev}}", rev=current_revision, cause=e)
                getall(hg, please_stop)
    finally:
        please_stop.go()
        Log.alert("DONE!")


def main():
    global MIN_DATE
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        MIN_DATE = Math.min(Date(settings.min_date), Date("01 MAR 2015"))

        stopper = Signal()
        Thread.run("backfill repo", worker, settings.hg, please_stop=stopper)
        Thread.wait_for_shutdown_signal(stopper, allow_exit=True)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

