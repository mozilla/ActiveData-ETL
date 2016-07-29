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

from datetime import timedelta
import os
import subprocess
import urllib
from pyLibrary.env import elasticsearch

from pyLibrary.sql.sql import find_holes
from pyLibrary import convert
from pyLibrary.debugs import startup
from pyLibrary.maths.randoms import Random
from pyLibrary.sql.mysql import MySQL
from pyLibrary.env.files import File
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import jx
from pyLibrary.strings import between
from pyLibrary.dot import coalesce, wrap
from pyLibrary.thread.multithread import Multithread
from pyLibrary.times.timer import Timer


DEBUG = True

TEMPLATE_FILE = File("C:/Users/klahnakoski/git/datazilla-alerts/tests/resources/hg/changeset_nofiles.template")

def pull_repo(repo):
    if not File(os.path.join(repo.directory, ".hg")).exists:
        File(repo.directory).delete()

        # REPO DOES NOT EXIST, CLONE IT
        with Timer("Clone hg log for {{name}}", {"name":repo.name}):
            proc = subprocess.Popen(
                ["hg", "clone", repo.url, File(repo.directory).filename],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=-1
            )
            try:
                while True:
                    line = proc.stdout.readline()
                    if line.startswith("abort:"):
                        Log.error("Can not clone {{repos.url}}, because {{problem}}", {
                            "repos": repo,
                            "problem": line
                        })
                    if line == '':
                        break
                    Log.note("Mercurial cloning: {{status}}", {"status": line})
            finally:
                proc.wait()


    else:
        hgrc_file = File(os.path.join(repo.directory, ".hg", "hgrc"))
        if not hgrc_file.exists:
            hgrc_file.write("[paths]\ndefault = " + repo.url + "\n")

        # REPO EXISTS, PULL TO UPDATE
        with Timer("Pull hg log for {{name}}", {"name":repo.name}):
            proc = subprocess.Popen(
                ["hg", "pull", "--cwd", File(repo.directory).filename],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=-1
            )
            (output, _) = proc.communicate()

            if output.find("abort: repository default not found!") >= 0:
                File(repo.directory).delete()
                pull_repo(repo)
                return
            if output.find("abort: abandoned transaction found") >= 0:
                Log.error("Problem pulling repos, try \"hg recover\"\n{{reason|indent}}", {"reason": output})
                File(repo.directory).delete()
                pull_repo(repo)
                return
            if output.find("abort: ") >= 0:
                Log.error("Problem with pull {{reason}}", {"reason": between(output, "abort:", "\n")})

            Log.note("Mercurial pull results:\n{{pull_results}}", {"pull_results": output})



def get_changesets(date_range=None, revision_range=None, repo=None):
    # GET ALL CHANGESET INFO
    args = [
        "hg",
        "log",
        "--cwd",
        File(repo.directory).filename,
        "-v",
        # "-p",   # TO GET PATCH CONTENTS
        "--style",
        TEMPLATE_FILE.filename
    ]

    if date_range is not None:
        if date_range.max == None:
            if date_range.min == None:
                drange = ">0 0"
            else:
                drange = ">" + unicode(convert.datetime2unix(date_range.min)) + " 0"
        else:
            if date_range.min == None:
                drange = "<" + unicode(convert.datetime2unix(date_range.max) - 1) + " 0"
            else:
                drange = unicode(convert.datetime2unix(date_range.min)) + " 0 to " + unicode(convert.datetime2unix(date_range.max) - 1) + " 0"

        args.extend(["--date", drange])


    if revision_range is not None:
        args.extend(["-r", str(revision_range.min) + ":" + str(revision_range.max)])

    proc = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1
    )

    def iterator():
        try:
            while True:
                try:
                    line = proc.stdout.readline()
                    if line == '':
                        proc.wait()
                        if proc.returncode:
                            Log.error("Unable to pull hg log: return code {{return_code}}", {
                                "return_code": proc.returncode
                            })
                        return
                except Exception, e:
                    Log.error("Problem getting another line", e)

                if line.strip() == "":
                    continue
                Log.note(line)


                # changeset = "{date|hgdate|urlescape}\t{node}\t{rev}\t{author|urlescape}\t{branches}\t\t\t\t{p1rev}\t{p1node}\t{parents}\t{children}\t{tags}\t{desc|urlescape}\n"
                # branch = "{branch}%0A"
                # parent = "{parent}%0A"
                # tag = "{tag}%0A"
                # child = "{child}%0A"
                (
                    date,
                    node,
                    rev,
                    author,
                    branches,
                    files,
                    file_adds,
                    file_dels,
                    p1rev,
                    p1node,
                    parents,
                    children,
                    tags,
                    desc
                ) = (urllib.unquote(c) for c in line.split("\t"))

                file_adds = set(file_adds.split("\n")) - {""}
                file_dels = set(file_dels.split("\n")) - {""}
                files = set(files.split("\n")) - set()
                doc = {
                    "repos": repo.name,
                    "date": convert.unix2datetime(convert.value2number(date.split(" ")[0])),
                    "node": node,
                    "revision": rev,
                    "author": author,
                    "branches": set(branches.split("\n")) - {""},
                    "file_changes": files - file_adds - file_dels - {""},
                    "file_adds": file_adds,
                    "file_dels": file_dels,
                    "parents": set(parents.split("\n")) - {""} | {p1rev+":"+p1node},
                    "children": set(children.split("\n")) - {""},
                    "tags": set(tags.split("\n")) - {""},
                    "description": desc
                }
                doc = elasticsearch.scrub(doc)
                yield doc
        except Exception, e:
            if isinstance(e, ValueError) and e.message.startswith("need more than "):
                Log.error("Problem iterating through log ({{message}})", {
                    "message": line
                }, e)


            Log.error("Problem iterating through log", e)

    return iterator()


def update_repo(repo, settings):
    with MySQL(settings.database) as db:
        try:
            pull_repo(repo)

            # GET LATEST DATE
            existing_range = db.query("""
                        SELECT
                            max(`date`) `max`,
                            min(`date`) `min`,
                            min(revision) min_rev,
                            max(revision) max_rev
                        FROM
                            changesets
                        WHERE
                            repos={{repos}}
                    """, {"repos": repo.name})[0]

            ranges = wrap([
                {"min": coalesce(existing_range.max, convert.milli2datetime(0)) + timedelta(days=1)},
                {"max": existing_range.min}
            ])

            for r in ranges:
                for g, docs in jx.groupby(get_changesets(date_range=r, repo=repo), size=100):
                    for doc in docs:
                        doc.file_changes = None
                        doc.file_adds = None
                        doc.file_dels = None
                        doc.description = doc.description[0:16000]

                    db.insert_list("changesets", docs)
                    db.flush()

            missing_revisions = find_holes(db, "changesets", "revision",  {"min": 0, "max": existing_range.max_rev + 1}, {"term": {"repos": repo.name}})
            for _range in missing_revisions:
                for g, docs in jx.groupby(get_changesets(revision_range=_range, repo=repo), size=100):
                    for doc in docs:
                        doc.file_changes = None
                        doc.file_adds = None
                        doc.file_dels = None
                        doc.description = doc.description[0:16000]

                    db.insert_list("changesets", docs)
                    db.flush()



        except Exception, e:
            Log.warning("Failure to pull from {{repos.name}}", {"repos": repo}, e)


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        with Multithread(update_repo, threads=10, outbound=False) as multi:
            for repo in Random.combination(settings.param.repos):
                multi.execute([{"repos": repo, "settings": settings}])
    finally:
        Log.stop()


main()


# hg log -v -l 20 --template "{date}\t{node}\t{rev}\t{author|urlescape}\t{branches}\t{files}\t{file_adds}\t{file_dels}\t{parents}\t{tags}\t{desc|urlescape}\n"
#
#
#
#
# hg log -v -l 20 --style "C:\Users\klahnakoski\git\datazilla-alerts\tests\resources\hg\changeset.template"


