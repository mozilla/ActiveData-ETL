# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from copy import copy

from BeautifulSoup import BeautifulSoup

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.exceptions import suppress_exception
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, set_default
from pyLibrary.env import elasticsearch, http
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import SECOND, DAY
from mohg.hg_mozilla_org import DEFAULT_LOCALE


EXTRA_WAIT_TIME = 20 * SECOND  # WAIT TIME TO SEND TO AWS, IF WE wait_forever
OLD_BRANCH = DAY


@use_settings
def get_branches(hg, branches, use_cache=True, settings=None):
    if not settings.branches or not use_cache:
        found_branches = _get_branches_from_hg(hg)

        es = elasticsearch.Cluster(settings=branches).get_or_create_index(settings=branches)
        es.add_alias()
        es.extend({"id": b.name + " " + b.locale, "value": b} for b in found_branches)
        es.flush()
        return found_branches

    # TRY ES
    try:
        es = elasticsearch.Cluster(settings=branches).get_index(settings=branches)
        query = {
            "query": {"match_all": {}},
            "size": 20000
        }

        docs = es.search(query).hits.hits._source
        # IF IT IS TOO OLD, THEN PULL FROM HG
        oldest = Date(Math.MAX(docs.etl.timestamp))
        if Date.now() - oldest > OLD_BRANCH:
            return get_branches(use_cache=False, settings=settings)

        try:
            return UniqueIndex(["name", "locale"], data=docs, fail_on_dup=False)
        except Exception, e:
            Log.error("Bad branch in ES index", cause=e)
    except Exception, e:
        if "Can not find index " in e:
            return get_branches(use_cache=False, settings=settings)
        Log.error("problem getting branches", cause=e)


@use_settings
def _get_branches_from_hg(settings):
    # GET MAIN PAGE
    response = http.get(settings.url)
    doc = BeautifulSoup(response.all_content)

    all_repos = doc("table")[1]
    branches = UniqueIndex(["name", "locale"], fail_on_dup=False)
    for i, r in enumerate(all_repos("tr")):
        dir, name = [v.text.strip() for v in r("td")]

        b = _get_single_branch_from_hg(settings, name, dir.lstrip("/"))
        branches.extend(b)

    # branches.add(set_default({"name": "release-mozilla-beta"}, branches["mozilla-beta", DEFAULT_LOCALE]))
    for b in list(branches["mozilla-beta", ]):
        branches.add(set_default({"name": "release-mozilla-beta"}, b))  # THIS IS THE l10n "name"
        b.url = "https://hg.mozilla.org/releases/mozilla-beta"          # THIS IS THE

    for b in list(branches["mozilla-release", ]):
        branches.add(set_default({"name": "release-mozilla-release"}, b))

    for b in list(branches["mozilla-aurora", ]):
        if b.locale == "en-US":
            continue
        branches.add(set_default({"name": "comm-aurora"}, b))
        # b.url = "https://hg.mozilla.org/releases/mozilla-aurora"

    for b in list(branches):
        if b.name.startswith("mozilla-esr"):
            branches.add(set_default({"name": "release-" + b.name}, b))  # THIS IS THE l10n "name"
            b.url = "https://hg.mozilla.org/releases/" + b.name

    #CHECKS
    for b in branches:
        if b.name != b.name.lower():
            Log.error("Expecting lowercase name")
        if not b.locale:
            Log.error("Not expected")
        if not b.url.startswith("http"):
            Log.error("Expecting a valid url")
        if not b.etl.timestamp:
            Log.error("Expecting a timestamp")

    return branches


def _get_single_branch_from_hg(settings, description, dir):
    if dir == "users":
        return []
    response = http.get(settings.url + "/" + dir)
    doc = BeautifulSoup(response.all_content)

    output = []
    try:
        all_branches = doc("table")[0]
    except Exception, _:
        return []

    for i, b in enumerate(all_branches("tr")):
        if i == 0:
            continue  # IGNORE HEADER
        columns = b("td")

        with suppress_exception:
            path = columns[0].a.get('href')
            if path == "/":
                continue

            name, desc, last_used = [c.text for c in columns][0:3]
            detail = Dict(
                name=name.lower(),
                locale=DEFAULT_LOCALE,
                parent_name=description,
                url=settings.url + path,
                description=desc,
                last_used=Date(last_used),
                etl={"timestamp": Date.now()}
            )
            if detail.description == "unknown":
                detail.description = None

            # SOME BRANCHES HAVE NAME COLLISIONS, IGNORE LEAST POPULAR
            if path in [
                "/projects/dxr/",                   # moved to webtools
                "/build/compare-locales/",          # ?build team likes to clone?
                "/build/puppet/",                   # ?build team likes to clone?
                "/SeaMonkey/puppet/",               # looses the popularity contest
                "/releases/gaia-l10n/v1_2/en-US/",  # use default branch
                "/releases/gaia-l10n/v1_3/en-US/",  # use default branch
                "/releases/gaia-l10n/v1_4/en-US/",  # use default branch
                "/releases/gaia-l10n/v2_0/en-US/",  # use default branch
                "/releases/gaia-l10n/v2_1/en-US/"   # use default branch
                "/build/autoland/"
            ]:
                continue

            # MARKUP BRANCH IF LOCALE SPECIFIC
            if path.startswith("/l10n-central"):
                _path = path.strip("/").split("/")
                detail.locale = _path[-1]
                detail.name = "mozilla-central"
            elif path.startswith("/releases/l10n/"):
                _path = path.strip("/").split("/")
                detail.locale = _path[-1]
                detail.name = _path[-2].lower()
            elif path.startswith("/releases/gaia-l10n/"):
                _path = path.strip("/").split("/")
                detail.locale = _path[-1]
                detail.name = "gaia-" + _path[-2][1::]
            elif path.startswith("/weave-l10n"):
                _path = path.strip("/").split("/")
                detail.locale = _path[-1]
                detail.name = "weave"

            Log.note("Branch {{name}} {{locale}}", name=detail.name, locale=detail.locale)
            output.append(detail)

    return output


def main():

    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        branches = _get_branches_from_hg(settings.hg)

        es = elasticsearch.Cluster(settings=settings.hg.branches).get_or_create_index(settings=settings.hg.branches)
        es.add_alias()
        es.extend({"id": b.name + " " + b.locale, "value": b} for b in branches)
        Log.alert("DONE!")
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
