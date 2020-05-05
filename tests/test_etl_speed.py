# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from mo_logs import Log
from mo_dots import Data
from mo_http.big_data import GzipLines
from mo_files import File
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times.timer import Timer
from activedata_etl.transforms import get_test_result_content
from activedata_etl.transforms.unittest_logs_to_sink import process_unittest_in_s3


class TestEtlSpeed(FuzzyTestCase):
    """
    TEMPORARY TEST TO IDENTIFY ETL SPEED ISSUES
    """

    def test_51586(self):
        debug_settings = {
            "trace": True,
            "cprofile": {
                "enabled": True,
                "filename": "tests/results/test_51586_profile.tab"
            }
        }
        Log.start(debug_settings)

        source_key = "51586_5124145.52"
        content = File("tests/resources/51586_5124145.52.json.gz").read_bytes()
        source = Data(read_lines=lambda: GzipLines(content))
        with Accumulator(File("tests/results/51586_5124145.52.json")) as destination:
            with Timer("ETL file"):
                process_unittest_in_s3(source_key, source, destination, please_stop=None)
        Log.stop()


    def test_read_blobber_file(self):
        debug_settings = {
            "trace": True,
            "cprofile": {
                "enabled": True,
                "filename": "tests/results/test_read_blobber_file_profile.tab"
            }
        }
        Log.start(debug_settings)
        get_test_result_content(
            0,
            "jetpack-package_raw.log",
            "http://mozilla-releng-blobs.s3.amazonaws.com/blobs/try/sha512/2d6892a08b84499c0e8cc0b81a32c830f6505fc2812a61e136ae4eb2ecfde0aac3e6358e9d27b76171869e0cc4368418e4dfca9378e69982681213354a2057ac"
        )
        Log.stop()



class Accumulator(object):

    def __init__(self, file):
        self.acc=[]
        self.file = file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.write(self.acc)


    def extend(self, values):
        self.acc.extend(value2json(v) for v in values)
