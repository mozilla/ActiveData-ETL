
# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

import requests

from activedata_etl.imports.resource_usage import normalize_resource_usage
from pyLibrary import convert
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestResourceUsage(FuzzyTestCase):
    def test_transform(self):
        url = "http://mozilla-releng-blobs.s3.amazonaws.com/blobs/mozilla-inbound/sha512/02967f2823389e355b90efacd27953fb02953741c79f01648ba369674ec1e6ff888d9bf435654c2826dde51a91dd5313b73f61a2d32a68b1a7f7ad6d285720df"

        normalize_resource_usage(url)

