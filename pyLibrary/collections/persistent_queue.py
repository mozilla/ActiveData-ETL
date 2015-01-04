# encoding: utf-8
#
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
from pyLibrary.debugs.logs import Log
from pyLibrary.env.files import File
from pyLibrary.maths.randoms import Random
from pyLibrary.structs import Dict, wrap
from pyLibrary.thread.threads import Lock, Thread, Signal


DEBUG = True


class PersistentQueue(object):
    """
    THREAD-SAFE, PERSISTENT QUEUE
    IT IS IMPORTANT YOU commit() or close(), OTHERWISE NOTHING COMES OFF THE QUEUE
    """

    def __init__(self, _file):
        """
        file - USES FILE FOR PERSISTENCE
        """
        self.file = File.new_instance(_file)
        self.lock = Lock("lock for persistent queue using file " + self.file.name)
        self.please_stop = Signal()
        self.db = Dict()
        self.pending = []

        if self.file.exists:
            for line in self.file:
                try:
                    delta = convert.json2value(line)
                    apply_delta(self.db, delta)
                except:
                    pass
            if self.db.status.start == None:  # HAPPENS WHEN ONLY ADDED TO QUEUE, THEN CRASH
                self.db.status.start = 0
            self.start = self.db.status.start
            if DEBUG:
                Log.note("Persistent queue {{name}} found with {{num}} items", {"name": self.file.abspath, "num": len(self)})
        else:
            self.db.status = Dict(
                start=0,
                end=0
            )
            self.start = self.db.status.start
            if DEBUG:
                Log.note("New persistent queue {{name}}", {"name": self.file.abspath})

    def _apply(self, delta):
        delta = wrap(delta)
        apply_delta(self.db, delta)
        self.pending.append(delta)

    def __iter__(self):
        """
        BLOCKING ITERATOR
        """
        while not self.please_stop:
            try:
                value = self.pop()
                if value is not Thread.STOP:
                    yield value
            except Exception, e:
                Log.warning("Tell me about what happened here", e)
        if DEBUG:
            Log.note("queue iterator is done")

    def add(self, value):
        with self.lock:
            if self.closed:
                Log.error("Queue is closed")

            if value is Thread.STOP:
                if DEBUG:
                    Log.note("Stop is seen in persistent queue")
                self.please_stop.go()
                return

            self._apply({"add": {str(self.db.status.end): value}})
            self.db.status.end += 1
            self._apply({"add": {"status.end": self.db.status.end}})
            self._commit()
        return self

    def __len__(self):
        with self.lock:
            return self.db.status.end - self.start

    def __getitem__(self, item):
        return self.db[str(item+self.start)]

    def pop(self):
        with self.lock:
            while not self.please_stop:
                if self.db.status.end > self.start:
                    value = self.db[str(self.start)]
                    self.start += 1
                    return value

                try:
                    self.lock.wait()
                except Exception, e:
                    pass
            if DEBUG:
                Log.note("persistent queue stopped")
            return Thread.STOP

    def pop_all(self):
        """
        NON-BLOCKING POP ALL IN QUEUE, IF ANY
        """
        with self.lock:
            if self.please_stop:
                return [Thread.STOP]
            if self.db.status.end == self.start:
                return []

            output = []
            for i in range(self.start, self.db.status.end):
                output.append(self.db[str(i)])

            self.start = self.db.status.end
            return output

    def rollback(self):
        with self.lock:
            if self.closed:
                return
            self.start = self.db.status.start

    def commit(self):
        with self.lock:
            if self.closed:
                Log.error("Queue is closed, commit not allowed")

            old_start = self.db.status.start
            try:

                if self.db.status.end - self.start < 10 or Random.range(1000) == 0:  # FORCE RE-WRITE TO LIMIT FILE SIZE
                    # SIMPLY RE-WRITE FILE
                    if DEBUG:
                        Log.note("Re-write persistent queue")
                    self.db.status.start = self.start
                    self.file.write(convert.value2json({"add": self.db}) + "\n")
                    self.pending = []
                else:
                    self._apply({"add": {"status.start": self.start}})
                    for i in range(self.db.status.start, self.start):
                        self._apply({"remove": str(i)})

                    self._commit()
            except Exception, e:
                self.db.status.start = old_start  # REALLY DOES NOTHING, WE LOST DATA AT THIS POINT
                raise e

    def _commit(self):
        self.file.append("\n".join(convert.value2json(p) for p in self.pending))
        self.pending = []

    def close(self):
        self.please_stop.go()
        with self.lock:
            if self.db is None:
                return

            if self.db.status.end == self.start:
                if DEBUG:
                    Log.note("persistent queue clear and closed")
                self.file.delete()
            else:
                if DEBUG:
                    Log.note("persistent queue closed with {{num}} items left", {"num": len(self)})
                for i in range(self.db.status.start, self.start):
                    self._apply({"remove": str(i)})

                self.db.status.start = self.start
                self.file.write(convert.value2json({"add": self.db}) + "\n")

            self.db = None

    @property
    def closed(self):
        with self.lock:
            return self.db is None


def apply_delta(value, delta):
    if delta.add:
        for k, v in delta.add.items():
            value[k] = v
    elif delta.remove:
        value[delta.remove] = None
