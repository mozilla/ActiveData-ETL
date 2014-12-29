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
from pyLibrary.collections import MAX, MIN
from pyLibrary.collections.persistent_queue import PersistentQueue
from pyLibrary import aws
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.env.pulse import Pulse
from pyLibrary.maths import Math
from pyLibrary.queries import Q
from pyLibrary.structs import set_default, wrap, unwrap
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration
from testlog_etl import etl2key


PING_PERIOD = Duration.MINUTE
SYNCHRONIZATION_KEY = "0.json"



class SynchState(object):
    def __init__(self, synch):
        self.synch = synch
        self.pinger_thread = None
        self.next_key = 1
        self.ping_time = Date.now()
        self.source_key = 0


    def startup(self):
        """
        synch HAS read() AND write() SO SEPARATE INSTANCES CAN DETERMINE IF OTHERS ARE ALIVE
        RAISE EXCEPTION IF SOME OTHER INSTANCE HAS BEEN DETECTED
        RETURN START OF COUNT (always >=1)
        """

        try:
            json = self.synch.read()
            if json == None:
                Log.note("{{synchro_key}} does not exist.  Starting.", {"synchro_key": SYNCHRONIZATION_KEY})
                return

            last_run = convert.json2value(json)
            self.next_key = last_run.next_key
            self.source_key = last_run.source_key
            if last_run.action == "shutdown":
                Log.note("{{synchro_key}} shutdown detected.  Starting at {{num}}", {
                    "synchro_key": SYNCHRONIZATION_KEY,
                    "num": self.next_key
                })
            else:
                resume_time = Date(last_run.timestamp)+(PING_PERIOD*5)
                Log.note("Shutdown not detected, waiting until {{time}} to see if existing pulse_logger is running...", {"time": resume_time})
                while resume_time > Date.now():
                    Thread.sleep(seconds=10)
                    json = self.synch.read()
                    if json == None:
                        Log.note("{{synchro_key}} disappeared!  Starting over.", {"synchro_key": SYNCHRONIZATION_KEY})
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
        while not please_stop:
            Log.note("pinger starting sleep")
            Thread.sleep(till=Date(self.ping_time) + PING_PERIOD, please_stop=please_stop)
            if please_stop:
                break
            if Date.now() < Date(self.ping_time) + PING_PERIOD:
                continue
            self.ping()
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


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            with aws.s3.Bucket(settings.destination) as bucket:

                if settings.param.debug:
                    if settings.source.durable:
                        Log.error("Can not run in debug mode with a durable queue")
                    synch = SynchState(bucket.get_key(SYNCHRONIZATION_KEY))
                else:
                    synch = SynchState(bucket.get_key(SYNCHRONIZATION_KEY))
                    if settings.source.durable:
                        synch.startup()

                queue = PersistentQueue("pulse-logger-queue.json")
                if queue:
                    last_item = queue[len(queue)-1]
                    synch.source_key = last_item._meta.count+1


                with Pulse(settings.source, queue=queue, start=synch.source_key):
                    def log_loop(please_stop):
                        for i, g in Q.groupby(queue, size=settings.param.size):
                            etl_header = wrap({
                                "name": "Pulse block",
                                "bucket": settings.destination.bucket,
                                "timestamp": Date.now().milli,
                                "id": synch.next_key,
                                "source": {
                                    "id": unicode(MIN(g.select("_meta.count"))),
                                    "name": "pulse.mozilla.org"
                                },
                                "type": "aggregation"
                            })
                            full_key = etl2key(etl_header)
                            try:
                                output = [etl_header]
                                output.extend(
                                    set_default(
                                        {"etl": {
                                            "name":"Pulse block",
                                            "bucket": settings.destination.bucket,
                                            "timestamp": Date.now().milli,
                                            "id": synch.next_key,
                                            "source": {
                                                "name":"pulse.mozilla.org",
                                                "timestamp": Date(d._meta.sent).milli,
                                                "id": d._meta.count
                                            }
                                        }},
                                        d.payload
                                    )
                                    for i, d in enumerate(g)
                                )
                                bucket.write(full_key, "\n".join(convert.value2json(d) for d in output))
                                synch.advance()
                                synch.source_key = MAX(g.select("_meta.count"))+1
                                synch.ping()
                                queue.commit()
                                Log.note("Wrote {{num}} pulse messages to bucket={{bucket}}, key={{key}} ", {
                                    "num": len(g),
                                    "bucket": bucket.name,
                                    "key": full_key
                                })
                            except Exception, e:
                                queue.rollback()
                                if not queue.closed:
                                    Log.warning("Problem writing {{key}} to S3", {"key": full_key}, e)

                            if please_stop:
                                break
                        Log.note("log_loop() completed on it's own")

                    thread = Thread.run("pulse log loop", log_loop)
                    Thread.wait_for_shutdown_signal()

                Log.note("starting shutdown")
                thread.stop()
                thread.join()
                queue.close()
                Log.note("write shutdown state to S3")
                synch.shutdown()

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
