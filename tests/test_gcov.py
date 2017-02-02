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

import unittest

from activedata_etl.transforms import gcov_to_es
from pyLibrary import convert
from pyLibrary.dot import Null
from pyLibrary.env.files import File


class TestGcov(unittest.TestCase):
    def test_parsing(self):
        gcov_to_es.process_directory(
            source_dir="tests/resources/ccov/atk",
            destination=Destination("results/ccov/gcov_parsing_result.txt"),
            task_cluster_record=Null,
            file_etl=Null
        )


class Destination(object):

    def __init__(self, filename):
        self.file = File(filename)

    def extend(self, values, overwrite=False):
        if overwrite:
            self.file.write("\n".join(map(convert.value2json, values)) + "\n")
        else:
            self.file.append("\n".join(map(convert.value2json, values)) + "\n")

