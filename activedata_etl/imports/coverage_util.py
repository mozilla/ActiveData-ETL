# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (klahnakoski@mozilla.com)

from __future__ import division
from __future__ import unicode_literals

from jx_python import jx
from mo_dots import listwrap
from mo_logs import Log, Except
from mo_times import Timer
from mo_http import http

TUID_BLOCK_SIZE = 1000
DEBUG = True
LANGUAGE_MAPPINGS = [
    ("c/c++", (".c", ".cpp", ".h", ".cc", ".cxx", ".hh", ".hpp", ".hxx")),
    ("javascript", (".js", ".jsm", ".xul", ".xml", ".html", ".xhtml")),
    ("python", (".py",)),
    ("java", ("java",)),
]


def tuid_batches(source_key, task_cluster_record, resources, iterator, path="file"):
    # RETURN AN ITERATOR WITH COVERAGE RECORDS ANNOTATED WITH TUIDS

    def has_tuids(s):
        return s[path].is_firefox and (
            s[path].total_covered != 0 or s[path].total_uncovered != 0
        )

    def _annotate_sources(sources):
        """
        :param sources: LIST OF COVERAGE SOURCE STRUCTURES TO MARKUP
        :return: NOTHING, sources ARE MARKED UP
        """
        try:
            branch = task_cluster_record.repo.branch.name
            revision = task_cluster_record.repo.changeset.id[:12]
            sources = listwrap(sources)
            filenames = [s[path].name for s in sources if has_tuids(s)]

            with Timer("markup sources for {{num}} files", {"num": len(filenames)}, too_long=1):
                # WHAT DO WE HAVE
                found = resources.tuid_mapper.get_tuids(branch, revision, filenames)
                if found == None:
                    return  # THIS IS A FAILURE STATE, AND A WARNING HAS ALREADY BEEN RAISED, DO NOTHING

                for source in sources:
                    if (
                        DEBUG
                        and source[path].total_covered + source[path].total_uncovered
                        > 100000
                    ):
                        Log.warning(
                            "lines={{num}}, file={{name}}",
                            name=source[path].name,
                            num=source[path].total_covered
                            + source[path].total_uncovered,
                        )

                    if not has_tuids(source):
                        continue
                    line_to_tuid = found.get(source[path].name)
                    if line_to_tuid != None:
                        source[path].tuid_covered = [
                            line_to_tuid[line]
                            for line in source.file.covered
                            if line_to_tuid[line]
                        ]
                        source[path].tuid_uncovered = [
                            line_to_tuid[line]
                            for line in source.file.uncovered
                            if line_to_tuid[line]
                        ]
        except Exception as e:
            e = Except.wrap(e)
            resources.tuid_mapper.enabled = False
            if "database is closed" not in e:
                Log.warning(
                    "failure with TUID mapping with {{key}}",
                    key=source_key,
                    cause=e
                )

    for g, records in jx.chunk(iterator, size=TUID_BLOCK_SIZE):
        _annotate_sources(records)
        for r in records:
            yield r


def download_file(url, destination):
    with open(destination, "w+b") as tempfile:
        stream = http.get(url).raw
        try:
            for b in iter(lambda: stream.read(8192), b""):
                tempfile.write(b)
        finally:
            stream.close()
