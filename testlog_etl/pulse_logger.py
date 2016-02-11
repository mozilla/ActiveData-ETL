# encoding: utf-8
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
from pyLibrary.collections import MAX, MIN
from pyLibrary.collections.persistent_queue import PersistentQueue
from pyLibrary import aws
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.env import pulse
from pyLibrary.queries import qb
from pyLibrary.dot import set_default, coalesce
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from testlog_etl.synchro import SynchState, SYNCHRONIZATION_KEY

# ONLY DEPLOY OFF THE pulse-logger branch

def log_loop(settings, synch, queue, bucket, please_stop):
    queue_name = coalesce(settings.work_queue, settings.notify)
    if queue_name:
        work_queue = aws.Queue(queue_name)
    else:
        work_queue = None

    try:
        for i, g in qb.groupby(queue, size=settings.param.size):
            Log.note(
                "Preparing {{num}} pulse messages to bucket={{bucket}}",
                num=len(g),
                bucket=bucket.name
            )

            full_key = unicode(synch.next_key) + ":" + unicode(MIN(g.select("_meta.count")))
            try:
                output = [
                    set_default(
                        d,
                        {"etl": {
                            "name": "Pulse block",
                            "bucket": settings.destination.bucket,
                            "timestamp": Date.now().unix,
                            "id": synch.next_key,
                            "source": {
                                "name": "pulse.mozilla.org",
                                "id": d._meta.count,
                                "count": d._meta.count,
                                "message_id": d._meta.message_id,
                                "sent": Date(d._meta.sent),
                            },
                            "type": "aggregation"
                        }}
                    )
                    for i, d in enumerate(g)
                    if d != None  # HAPPENS WHEN PERSISTENT QUEUE FAILS TO LOG start
                ]
                bucket.write(full_key, "\n".join(convert.value2json(d) for d in output))
                synch.advance()
                synch.source_key = MAX(g.select("_meta.count")) + 1

                now = Date.now()
                if work_queue != None:
                    work_queue.add({
                        "bucket": bucket.name,
                        "key": full_key,
                        "timestamp": now.unix,
                        "date/time": now.format()
                    })

                synch.ping()
                queue.commit()
                Log.note(
                    "Wrote {{num}} pulse messages to bucket={{bucket}}, key={{key}} ",
                    num=len(g),
                    bucket=bucket.name,
                    key=full_key
                )
            except Exception, e:
                queue.rollback()
                if not queue.closed:
                    Log.warning("Problem writing {{key}} to S3", key=full_key, cause=e)

            if please_stop:
                break
    finally:
        if work_queue != None:
            work_queue.close()
    Log.note("log_loop() completed on it's own")


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        constants.set(settings.constants)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            with aws.s3.Bucket(settings.destination) as bucket:

                if settings.param.debug:
                    if settings.source.durable:
                        Log.error("Can not run in debug mode with a durable queue")
                    synch = SynchState(bucket.get_key(SYNCHRONIZATION_KEY, must_exist=False))
                else:
                    synch = SynchState(bucket.get_key(SYNCHRONIZATION_KEY, must_exist=False))
                    if settings.source.durable:
                        synch.startup()

                queue = PersistentQueue(settings.param.queue_file)
                if queue:
                    last_item = queue[len(queue) - 1]
                    synch.source_key = last_item._meta.count + 1

                with pulse.Consumer(settings=settings.source, target=None, target_queue=queue, start=synch.source_key):
                    Thread.run("pulse log loop", log_loop, settings, synch, queue, bucket)
                    Thread.wait_for_shutdown_signal(allow_exit=True)
                    Log.warning("starting shutdown")

                queue.close()
                Log.note("write shutdown state to S3")
                synch.shutdown()

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
