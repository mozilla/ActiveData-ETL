# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary import convert
from pyLibrary.debugs.logs import Log


class DummySink(object):

    def __init__(self):
        pass

    def add(self, value):
        json = convert.value2json(value)
        # Log.note("{{json}}", {"json": json})

    def extend(self, values):
        pass

    def keys(self, prefix=None):
        return set([])


    @property
    def name(self):
        return "DummySink"
