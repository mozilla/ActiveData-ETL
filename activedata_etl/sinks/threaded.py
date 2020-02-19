# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from mo_future import text
from mo_threads import ThreadedQueue


class Threaded(object):

    def __init__(self, sink):
        self.sink=sink
        self.queue = ThreadedQueue(name=" to "+sink.__class__.__name__, queue=sink, max_size=2000, batch_size=1000, silent=False)

    def keys(self, prefix):
        return self.sink.keys(prefix=prefix)

    def extend(self, documents):
        self.queue.extend(documents)

    def add(self, doc):
        self.queue.add(doc)
