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
from pyLibrary.collections.persistent_queue import PersistentQueue

from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.env import aws_s3
from pyLibrary.env.emailer import Emailer
from pyLibrary.env.files import File
from pyLibrary.env.pulse import Pulse
from pyLibrary.queries import Q
from pyLibrary.structs import Struct
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date

SYNCHRONIZATION_KEY = "0.json"
ALREADY_RUNNING = "Another instance of pulse_logger is running!"


def logger_startup(synch):
    """
    synch HAS read() AND write() SO SEPARATE INSTANCES CAN DETERMINE IF OTHERS ARE ALIVE
    RAISE EXCEPTION IF SOME OTHER INSTANCE HAS BEEN DETECTED
    RETURN START OF COUNT (always >=1)
    """

    start_time = Date.now().format()
    try:
        json = synch.read()
        last_run = convert.json2value(json)
        key = last_run.last_key
        if last_run.shutdown:
            start_time = last_run.shutdown
            Log.note("{{synchro_key}} exists.  Starting at {{start_time}}, {{num}}", {
                "start_time": start_time,
                "synchro_key=": SYNCHRONIZATION_KEY,
                "num": key
            })
        else:
            Log.note("Shutdown not detected, waiting 5minutes to see if existing pulse_logger is running...")
            for i in range(5 * 6):
                Thread.sleep(seconds=10)
                json = synch.read()
                last_run = convert.json2value(json)
                if last_run.shutdown:
                    Log.note("Shutdown detected!  Resuming...")
                    return last_run.last_key + 1, start_time
                if last_run.last_key > key:
                    Log.error(ALREADY_RUNNING)
                Log.note("No activity, still waiting...")
            Log.note("No activity after 5minutes.  Resuming...")

    except Exception, e:
        if isinstance(e, Except) and e.contains(aws_s3.READ_ERROR) or e.contains(ALREADY_RUNNING):
            Log.error("Can not start", e)
        Log.note("{{synchro_key}} does not exist.  Starting over {{start_time}}", {
            "start_time": start_time,
            "synchro_key=": SYNCHRONIZATION_KEY
        })
        key = 0

    synch.write(convert.value2json({
        "startup": start_time,
        "last_key": key
    }))
    return key + 1, start_time


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            with aws_s3.Bucket(settings.destination) as bucket:
                synch = bucket.get_key(SYNCHRONIZATION_KEY)

                if settings.param.debug:
                    if settings.source.durable:
                        Log.error("Can not run in debug mode with a durable queue")
                    start = 1
                    start_time = Date.now().format()
                else:
                    start, start_time = logger_startup(synch)

                key = Struct(value=start)
                queue = PersistentQueue("pulse-logger-queue.json")

                with Pulse(settings.source, queue=queue):
                    def log_loop(please_stop):
                        for i, g in Q.groupby(queue, size=settings.param.size):
                            key.value = i + start
                            full_key = unicode(start_time) + "." + "{0:09d}".format(key.value) + ".json"
                            try:
                                bucket.write(full_key, "\n".join(g))
                                synch.write(convert.value2json({
                                    "ping": start_time,
                                    "last_key": key.value
                                }))
                                queue.commit()
                                Log.note("Wrote {{num}} pulse messages to bucket={{bucket}}, key={{key}} ", {"num": len(g), "bucket": bucket.name, "key": full_key})
                            except Exception, e:
                                queue.rollback()
                                if not queue.closed:
                                    Log.warning("Problem writing {key}} to S3", {"key": full_key}, e)

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
                synch.write(convert.value2json({
                    "shutdown": start_time,
                    "last_key": key.value
                }))


    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
