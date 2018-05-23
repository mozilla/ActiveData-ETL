# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from mo_future import text_type
class Split(object):
    """
    SEND SAME DATA TO TWO DIFFERENT SINKS
    """

    def __init__(self, A, B):
        self.A=A
        self.B = B

    # ADD keys() SO ETL LOOP CAN FIND WHAT'S GETTING REPLACED
    def keys(self, prefix=None):
        return self.A.keys(prefix=prefix) | self.B.keys(prefix=prefix)

    def extend(self, documents):
        self.A.extend(documents)
        self.B.extend(documents)

    def add(self, doc):
        self.A.add(doc)
        self.B.add(doc)

