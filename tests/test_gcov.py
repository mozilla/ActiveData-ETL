# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (klahnakoski@mozilla.com)
#
from __future__ import division
from __future__ import unicode_literals

import gzip
import unittest

from mo_dots import Null

from activedata_etl.transforms import gcov_to_es


class TestGcov(unittest.TestCase):
    def test_parsing(self):
        destination = Destination("results/ccov/gcov_parsing_result.json.gz")

        gcov_to_es.process_directory(
            "tc.0:0.0",
            source_dir="tests/resources/ccov/atk",
            # source_dir="/home/marco/Documenti/FD/mozilla-central/build-cov-gcc",
            destination=destination,
            task_cluster_record=Null,
            file_etl=Null
        )

        self.assertEqual(destination.count, 81, "Expecting 81 records, got " + str(destination.count))


class Destination(object):

    def __init__(self, filename):
        self.filename = filename
        self.count = 0

    def write_lines(self, key, lines):
        archive = gzip.GzipFile(self.filename, mode='w')
        for l in lines:
            archive.write(l.encode("utf8"))
            archive.write(b"\n")
            self.count += 1
        archive.close()
