# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

# NEED TO BE NOTIFIED OF ID TO REPROCESS
# NEED TO BE NOTIFIED OF RANGE TO REPROCESS
# MUST SEND CONSEQUENCE DOWN THE STREAM SO OTHERS CAN WORK ON IT
from BeautifulSoup import BeautifulSoup
from pyLibrary import strings

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, Dict
from pyLibrary.dot.objects import dictwrap
from pyLibrary.env import elasticsearch, http
from pyLibrary.queries.unique_index import UniqueIndex
from pyLibrary.thread.threads import Thread, Signal
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration


EXTRA_WAIT_TIME = 20 * Duration.SECOND  # WAIT TIME TO SEND TO AWS, IF WE wait_forever


def get_branches(settings):
    # GET MAIN PAGE
    response = http.get(settings.url)
    doc = BeautifulSoup(response.all_content)

    all_repos = doc("table")[1]
    branches = []
    for i, r in enumerate(all_repos("tr")):
        dir, name = [v.text.strip() for v in r("td")]

        b = get_branch(settings, name, dir.lstrip("/"))
        branches.extend(b)
    return branches

def get_branch(settings, name, dir):
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

        try:
            path = columns[0].a.get('href')
            if path == "/":
                continue
            detail = Dict(
                name=columns[0].text,
                parent_name=name,
                url=settings.url + path,
                description=columns[1].text,
                last_used=Date(columns[2].text)
            )
            if detail.description == "unknown":
                detail.description = None

            Log.note("Branch\n{{branch|json|indent}}", branch=detail)
            output.append(detail)
        except Exception, _:
            pass

    return output


def main():

    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        branches = get_branches(settings.hg)

        es = elasticsearch.Cluster(settings=settings.hg.branches).get_or_create_index(settings=settings.hg.branches)
        es.add_alias()
        es.extend({"id": b.name, "value": b} for b in branches)
        Log.alert("DONE!")
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
