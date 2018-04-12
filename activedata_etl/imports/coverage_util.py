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
from pyLibrary.env import http

TUID_BLOCK_SIZE = 1000


def tuid_batches(task_cluster_record, resources, iterator):
    for g, records in jx.groupby(iterator, size=TUID_BLOCK_SIZE):
        resources.tuid_mapper.annotate_sources(task_cluster_record.repo.changeset.id, records)
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


