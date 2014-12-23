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
import dumbdbm

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.env.files import File
from pyLibrary.structs import Struct
from pyLibrary.thread.threads import Lock, Thread


DEBUG = True

class PersistentQueue(object):
    """
    THREAD-SAFE, TRANSACTIONAL, PERSISTENT QUEUE
    IT IS IMPORTANT YOU commit(), OTHERWISE NOTHING COMES OFF THE QUEUE
    """

    def __init__(self, file):
        """
        file - USES FILE FOR PERSISTENCE
        """
        self.file = File.new_instance(file)
        self.db = dumbdbm.open(self.file.abspath, "c")
        self.lock = Lock("lock for persistent queue using file " + self.file.name)

        try:
            self.status = convert.json2value(convert.utf82unicode(self.db[b"status"]))
            self.start=self.status.start

            if DEBUG:
                Log.note("Persistent queue {{name}} found with {{num}} items", {"name": self.file.abspath, "num": len(self)})
        except Exception, e:
            if DEBUG:
                Log.note("New persistent queue {{name}}", {"name": self.file.abspath})
            self.status = Struct(
                start=0,
                end=0
            )
            self.start=self.status.start


    def __iter__(self):
        """
        BLOCKING ITERATOR
        """
        while self.db is not None:
            try:
                value = self.pop()
                if value is not Thread.STOP:
                    yield value
            except Exception, e:
                Log.warning("Tell me about what happened here", e)
        Log.note("queue iterator is done")

    def add(self, value):
        with self.lock:
            if self.db is None:
                Log.error("Queue is closed")

            self.db[str(self.status.end)] = convert.unicode2utf8(convert.value2json(value))
            self.status.end += 1
            self.db[b"status"] = convert.unicode2utf8(convert.value2json(self.status))
            self.db._commit()
        return self

    def extend(self, values):
        with self.lock:
            if self.db is None:
                Log.error("Queue is closed")

            for v in values:
                self.db[str(self.status.end)] = convert.unicode2utf8(convert.value2json(v))
                self.status.end += 1
            self.db[b"status"] = convert.unicode2utf8(convert.value2json(self.status))
            self.db._commit()
        return self

    def __len__(self):
        with self.lock:
            return self.status.end - self.start

    def pop(self):
        with self.lock:
            while self.db is not None:
                if self.status.end > self.start:
                    value = convert.json2value(convert.utf82unicode(self.db[str(self.start)]))
                    self.start += 1
                    return value

                try:
                    self.lock.wait()
                except Exception, e:
                    pass

            Log.note("queue stopped")
            return Thread.STOP

    def pop_all(self):
        """
        NON-BLOCKING POP ALL IN QUEUE, IF ANY
        """
        with self.lock:
            if self.db is None:
                return [Thread.STOP]
            if self.status.end == self.start:
                return []

            output = []
            for i in range(self.start, self.status.end):
                output.append(convert.json2value(convert.utf82unicode(self.db[str(i)])))

            self.start = self.status.end
            return output

    def commit(self):
        if self.status.end==self.start:
            if DEBUG:
                Log.note("Clear persistent queue")
            self.db.close()
            self.file.set_extension("dat").delete()
            self.file.set_extension("dir").delete()
            self.file.set_extension("bak").delete()
            self.db = dumbdbm.open(self.file.abspath, 'n')
        else:
            if DEBUG:
                Log.note("{{num}} items removed from persistent queue, {{remaining}} remaining", {
                    "num": self.status.start - self.start,
                    "remaining": len(self)
                })
            for i in range(self.status.start, self.start):
                del self.db[str(i)]

            self.status.start = self.start
            self.db[b"status"] = convert.unicode2utf8(convert.value2json(self.status))
            self.db._commit()

    def close(self):
        with self.lock:
            self.commit()
            self.db.close()
            self.db = None

