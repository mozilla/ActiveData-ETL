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

from pyLibrary import convert
from pyLibrary.debugs.logs import Except, Log
from pyLibrary.env.files import File
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from testlog_etl.imports.buildbot import BuildbotTranslator

false = False
true = True

class TestBuildbotLogs(FuzzyTestCase):

    def __init__(self, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)

    def test_past_problems(self):
        t = BuildbotTranslator()

        builds = convert.json2value(File("tests/resources/buildbot.json").read())
        failures=[]
        for b in builds:
            try:
                t.parse(b)
            except Exception, e:
                e = Except.wrap(e)
                failures.append(e)
                Log.warning("problem", cause=e)

        if failures:
            Log.error("parsing problems")

