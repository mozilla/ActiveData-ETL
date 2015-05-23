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


class TestResult(object):
    def __init__(self, revision, test_results):
        self.revision = revision
        self.test_results = test_results
        self.changesets = []    # CHANGESETS THAT ARE UNIQUE TO THIS TEST RESULT
