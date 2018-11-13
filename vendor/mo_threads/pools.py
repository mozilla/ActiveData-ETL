# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
# THIS THREADING MODULE IS PERMEATED BY THE please_stop SIGNAL.
# THIS SIGNAL IS IMPORTANT FOR PROPER SIGNALLING WHICH ALLOWS
# FOR FAST AND PREDICTABLE SHUTDOWN AND CLEANUP OF THREADS

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from threading import Semaphore

from mo_dots import Data
from mo_logs import Log
from mo_threads import Queue
from mo_threads.signal import Signal
from mo_threads.threads import Thread, THREAD_STOP


class Pool(object):

    def __init__(self, name, size, please_stop=None, fail_on_error=False, show_warnings=True):
        self.limiter = Semaphore(size)
        self.queue = Queue("thread queue for " + name)
        self.results = Queue("results for " + name)
        self.fail_on_error = fail_on_error
        self.please_stop = Signal("stop pool for "+name)
        self.show_warnings = show_warnings
        self.thread = Thread.run(name + " worker", self.worker, please_stop=self.please_stop | please_stop)

    def run(self, name, target, *args, **kwargs):
        self.queue.add((name, target, args, kwargs))

    def _pool_thread(self, name, target, please_stop, args, kwargs):
        try:
            result = target(*args, please_stop=please_stop, **kwargs)
            self.results.add(Data(result=result))
        except Exception as e:
            self.results.add(Data(exception=e))
            if self.show_warnings:
                Log.warning("Thread {{name}} failed", name=name, cause=e)
            if self.fail_on_error:
                self.please_stop.go()
        finally:
            self.limiter.release()

    def worker(self, please_stop):
        while not please_stop:
            cmd = self.queue.pop(till=please_stop)
            if cmd is THREAD_STOP:
                self.please_stop.go()
                continue

            name, target, args, kwargs = cmd
            self.limiter.acquire()
            Thread.run(name, self._pool_thread, name=name, args=args, kwargs=kwargs)

    def join(self):
        """
        :return: list of all thread results (that have not been popped off of self.results
        """
        self.queue.add(THREAD_STOP)
        self.thread.join()

        results = list(self.results)
        if self.fail_on_error and any(r.exception for r in results):
            Log.error("thread failure", cause=[r.exception for r in results if r.exception])
        else:
            return results
