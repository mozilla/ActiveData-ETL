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
from mo_logs import Log
from mo_times import Timer
from pyLibrary.env import http

TUID_BLOCK_SIZE = 1000

LANGUAGE_MAPPINGS = [
    ("c/c++", (".c", ".cpp", ".h", ".cc", ".cxx", ".hh", ".hpp", ".hxx")),
    ("javascript", (".js", ".jsm", ".xul", ".xml", ".html", ".xhtml")),
    ("python", (".py",))
]


def tuid_batches(task_cluster_record, resources, iterator):
    def _annotate_sources(sources):
        """

        :param sources: LIST OF COVERAGE SOURCE STRUCTURES TO MARKUP
        :return: NOTHING, sources ARE MARKED UP
        """
        try:
            revision = task_cluster_record.repo.changeset.id[:12]
            sources = listwrap(sources)
            filenames = [s.file.name for s in sources if s.file.is_firefox and s.file.total_covered == 0 and s.file.total_uncovered == 0]

            with Timer("markup sources for {{num}} files", {"num": len(filenames)}):
                # WHAT DO WE HAVE
                found = resources.tuid_mapper.get_tuids(revision, filenames)
                if found == None:
                    return  # THIS IS A FAILURE STATE, AND A WARNING HAS ALREADY BEEN RAISED, DO NOTHING

                for source in sources:
                    line_to_tuid = found[source.file.name]
                    if line_to_tuid != None:
                        source.file.tuid_covered = [
                            line_to_tuid[line]
                            for line in source.file.covered
                            if line_to_tuid[line]
                        ]
                        source.file.tuid_uncovered = [
                            line_to_tuid[line]
                            for line in source.file.uncovered
                            if line_to_tuid[line]
                        ]
        except Exception as e:
            resources.tuid_mapper.enabled = False
            Log.warning("unexpected failure", cause=e)

    for g, records in jx.groupby(iterator, size=TUID_BLOCK_SIZE):
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


