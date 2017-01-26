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
from MoLogs import Log
from pyLibrary.thread.threads import Thread
from pyLibrary.thread.till import Till
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import MINUTE

PING_PERIOD = MINUTE
WAIT_FOR_ACTIVITY = PING_PERIOD * 2
SYNCHRONIZATION_KEY = "0"



class SynchState(object):
    def __init__(self, synch):
        """
        synch HAS read() AND write() SO SEPARATE INSTANCES CAN DETERMINE IF OTHERS ARE ALIVE
        RAISE EXCEPTION IF SOME OTHER INSTANCE HAS BEEN DETECTED
        RETURN START OF COUNT (always >=1)
        """
        self.synch = synch
        self.pinger_thread = None
        self.next_key = 1
        self.ping_time = Date.now()
        self.source_key = 0


    def startup(self):
        try:
            try:
                json = self.synch.read()
            except Exception:
                json = None

            if not json:
                Log.note("{{synchro_key}} does not exist.  Starting.", synchro_key=SYNCHRONIZATION_KEY)
                return

            last_run = convert.json2value(json)
            self.next_key = last_run.next_key
            self.source_key = last_run.source_key
            if last_run.action == "shutdown":
                Log.note("{{synchro_key}} shutdown detected.  Starting at {{num}}",
                    synchro_key= SYNCHRONIZATION_KEY,
                    num= self.next_key)
            else:
                resume_time = Date(last_run.timestamp) + WAIT_FOR_ACTIVITY
                Log.note("Shutdown not detected, waiting until {{time}} to see if existing pulse_logger is running...",  time= resume_time)
                while resume_time > Date.now():
                    (Till(seconds=10)).wait()
                    json = self.synch.read()
                    if json == None:
                        Log.note("{{synchro_key}} disappeared!  Starting over.",  synchro_key= SYNCHRONIZATION_KEY)
                        self._start()
                        self.pinger_thread = Thread.run("synch pinger", self._pinger)
                        return

                    self.next_key = last_run.next_key
                    self.source_key = last_run.source_key
                    if last_run.action == "shutdown":
                        Log.note("Shutdown detected!  Resuming...")
                        self._start()
                        self.pinger_thread = Thread.run("synch pinger", self._pinger)
                        return

                    if last_run.timestamp > self.ping_time:
                        Log.error("Another instance of pulse_logger is running!")
                    Log.note("No activity, still waiting...")
                Log.note("No activity detected!  Resuming...")
        except Exception, e:
            Log.error("Can not start", e)

        self._start()
        self.pinger_thread = Thread.run("synch pinger", self._pinger)


    def advance(self):
        self.next_key += 1


    def _start(self):
        self.ping_time = Date.now()
        self.synch.write(convert.value2json({
            "action": "startup",
            "next_key": self.next_key,
            "source_key": self.source_key,
            "timestamp": self.ping_time.milli
        }))

    def ping(self):
        self.ping_time = Date.now()
        self.synch.write(convert.value2json({
            "action": "ping",
            "next_key": self.next_key,
            "source_key": self.source_key,
            "timestamp": self.ping_time.milli
        }))

    def _pinger(self, please_stop):
        Log.note("pinger started")
        while not please_stop:
            (Till(self.ping_time + PING_PERIOD) | please_stop).wait()
            if please_stop:  # EXIT EARLY, OTHERWISE WE MAY OVERWRITE THE shutdown
                break
            if Date.now() < self.ping_time + PING_PERIOD:
                continue
            try:
                self.ping()
            except Exception, e:
                Log.warning("synchro.py could not ping", e)
        Log.note("pinger stopped")

    def shutdown(self):
        self.pinger_thread.stop()
        self.pinger_thread.join()
        self.synch.write(convert.value2json({
            "action": "shutdown",
            "next_key": self.next_key,
            "source_key": self.source_key,
            "timestamp": Date.now().milli
        }))
