# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from mohg.hg_mozilla_org import HgMozillaOrg, DEFAULT_LOCALE
from mo_dots import listwrap, unwrap, wrap, wrap_leaves
from mo_logs import startup, constants
from mo_logs import Log
from pyLibrary.env import elasticsearch
from mo_math import Math
from jx_elasticsearch.es17 import FromES
from mo_collections import UniqueIndex
from mo_threads import Thread, Signal
from mo_threads import Till
from mo_times.dates import Date


DEBUG = False
MIN_DATE = Date("01 MAR 2015")
SCAN_DONE = "etl.done_branch_scan"


current_revision = None

def get_frontier(hg):
    # FIND THE FRONTIER
    if DEBUG:
        Log.warning("Running in debug mode! Not all changesets processed!!")
    Log.note("Find the frontier")
    detailed = UniqueIndex(keys=("changeset.id", "branch.name", "branch.locale"), fail_on_dup=False)
    known = UniqueIndex(keys=("changeset.id", "branch.name", "branch.locale"), fail_on_dup=False)

    before = Date.now().unix
    while True:
        Log.note("Query ES for known changesets before {{before|datetime}}", before=before)
        query = {
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and": [
                    {"exists": {"field": "branch.name"}},
                    {"exists": {"field": "branch.locale"}},
                    {"range": {"changeset.date": {"gte": MIN_DATE, "lte": before}}}
                ]}
            }},
            "fields": ["branch.name", "branch.locale", "changeset.id", "parents", "changeset.date"],
            "sort": {"changeset.date": "desc"},
            "size": 100000 if not DEBUG else 2000,
        }
        docs = hg.es.search(query).hits.hits

        Log.note("Convert {{num}} docs to standard form", num=len(docs))
        for d in unwrap(docs):
            r = elasticsearch.scrub(wrap_leaves(d["fields"]))
            before = Math.min(r.changeset.date, before)
            detailed.add(r)
            parents = listwrap(r.parents)
            if len(parents) == 1:
                for p in parents:
                    known.add({"branch": r.branch, "changeset": {"id": p}})
            else:
                for p in parents:
                    known.add({"changeset": {"id": p}})

        if len(docs)<100000 or DEBUG:
            break

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


def getall(hg, es, please_stop):
    global current_revision

    query = {
        "query": {"filtered": {
            "query": {"match_all": {}},
            "filter": {"and": [
                {"term": {"changeset.id": current_revision.changeset.id}},
                {"term": {SCAN_DONE: True}}
            ]}
        }},
        "fields": ["changeset.id"],
        "size": 1
    }
    docs = hg.es.search(query).hits.hits
    if docs:  # ALREADY DID A SCAN ON THIS CHANGESET
        Log.note("Scan of {{changeset}} avoided!  Yay!", changeset=current_revision.changeset.id)
        return

    branches = hg.find_changeset(current_revision.changeset.id, please_stop)
    for b in branches:
        if please_stop:
            Log.error("Exit early")
        hg.get_revision(wrap({"changeset": {"id": current_revision.changeset.id}, "branch": b}))
    hg.es.flush()

    def markup(id, please_stop):
        # MARKUP ES TO INDICATE A SCAN WAS DONE FOR THIS CHANGESET
        errors = wrap([])
        #TODO: use the `retry_on_conflict` parameter
        while len(errors) < 3 and not please_stop:
            try:
                (Till(seconds=10)).wait()
                es.update({
                    "set": wrap_leaves({SCAN_DONE: True}),
                    "where": {"eq": {"changeset.id": id}}
                })
                return
            except Exception as e:
                errors += [e]

        Log.error("Can not seem to markup changeset as scanned", cause=errors)

    Thread.run("markup", markup, current_revision.changeset.id)


def backfill_repo(settings, please_stop):
    global current_revision
    hg = HgMozillaOrg(settings)
    es = FromES(kwargs=settings.repo)

    frontier = UniqueIndex(keys=("changeset.id", "branch.name", "branch.locale"), fail_on_dup=False)
    frontier |= get_frontier(hg)
    patch_es(hg.es, frontier)

    try:
        while not please_stop and frontier:
            current_revision = frontier.pop()
            if not current_revision.branch:
                getall(hg, es, please_stop)
                continue

            if not current_revision.branch.locale:
                current_revision.branch.locale = DEFAULT_LOCALE
            try:
                rev = hg.get_revision(current_revision)
                frontier.remove(rev)
            except Exception as e:
                Log.warning("can not get {{rev}}", rev=current_revision, cause=e)
                getall(hg, es, please_stop)
    finally:
        Log.alert("DONE!")
        please_stop.go()


def main():
    global MIN_DATE
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        MIN_DATE = Math.min(Date(settings.min_date), Date("01 MAR 2015"))

        stopper = Signal()
        Thread.run("backfill repo", backfill_repo, settings.hg, please_stop=stopper)
        Thread.wait_for_shutdown_signal(stopper, allow_exit=True)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

