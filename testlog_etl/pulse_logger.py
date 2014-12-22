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

from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.env import aws_s3
from pyLibrary.env.pulse import Pulse
from pyLibrary.queries import Q
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date

ALREADY_RUNNING = "Another instance of pulse_logger is running!"


def logger_startup(bucket):
    """
    FIRGURE OUT IF WE SHOULD START TAKING STUFF OFF QUEUE, AND WHAT NUMBER TO START COUNTING AT

    RETURN START OF COUNT (always >=1)
    """

    start_time = Date.now().milli
    try:


        json = bucket.read("0.json")
        last_run = convert.json2value(json)
        key = last_run.last_key
        if last_run.shutdown:
            start_time = last_run.shutdown
            Log.note("0.json exists.  Starting at {{start_time}}", {"start_time": start_time})
        else:
            Log.note("Shutdown not detected, waiting 5minutes to see if existing pulse_logger is running...")
            for i in range(5 * 6):
                Thread.sleep(seconds=10)
                json = bucket.read("0.json")
                last_run = convert.json2value(json)
                if last_run.shutdown:
                    Log.note("Shutdown detected!  Resuming...")
                    return last_run.last_key + 1
                if last_run.last_key > key:
                    Log.error(ALREADY_RUNNING)
                Log.note("No activity, still waiting...")
            Log.note("No activity after 5minutes.  Resuming...")

    except Exception, e:
        if isinstance(e, Except) and e.contains("S3 read error") or e.contains(ALREADY_RUNNING):
            Log.error("Can not start", e)
        Log.note("0.json does not exist.  Starting over {{start_time}}", {"start_time": start_time})
        key = 0

    bucket.write("0.json", convert.value2json({
        "startup": start_time,
        "last_key": key
    }))
    return key + 1, start_time


def logger_ping(bucket, start_time, key):
    bucket.write("0.json", convert.value2json({
        "ping": start_time,
        "last_key": key
    }))


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            with aws_s3.Bucket(settings.destination) as bucket:

                start, start_time = logger_startup(bucket)
                key = start
                with Pulse(settings.source) as pulse:
                    def log_loop(please_stop):
                        for i, g in Q.groupby(pulse.queue, size=settings.param.size):
                            key = i + start
                            full_key = unicode(key) + "." + unicode(start_time) + ".json"
                            bucket.write(full_key, "\n".join(g))
                            logger_ping(bucket, start_time, key)
                            Log.note("Wrote {{num}} pulse messages to bucket={{bucket}}, key={{key}} ", {"num": len(g), "bucket": bucket.name, "key": full_key})

                    thread = Thread.run("pulse log loop", log_loop)

                    Thread.sleep_forever()

                    Log.note("starting shutdown")
                    pulse.queue.close()
                    thread.stop()
                    thread.join()
                    Log.note("write 0.json")
                    bucket.write("0.json", convert.value2json({
                        "shutdown": start_time,
                        "last_key": key
                    }))


    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
