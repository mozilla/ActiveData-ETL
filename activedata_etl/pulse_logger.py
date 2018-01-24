# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from mo_future import text_type
from activedata_etl.synchro import SynchState, SYNCHRONIZATION_KEY
from mo_dots import set_default, coalesce, listwrap
from pyLibrary import aws
from mo_json import json2value, value2json
from pyLibrary.collections import MAX, MIN
from pyLibrary.collections.persistent_queue import PersistentQueue
from mo_logs import startup, constants
from mo_logs.exceptions import Except
from mo_logs import Log
from pyLibrary.env import pulse
from jx_python import jx
from mo_threads import Thread
from mo_times.dates import Date


# ONLY DEPLOY OFF THE pulse-logger BRANCH


def log_loop(settings, synch, queue, bucket, please_stop):
    queue_name = coalesce(settings.work_queue, settings.notify)
    if queue_name:
        work_queue = aws.Queue(queue_name)
    else:
        work_queue = None

    try:
        for i, g in jx.groupby(queue, size=settings.param.size):
            Log.note(
                "Preparing {{num}} pulse messages to bucket={{bucket}}",
                num=len(g),
                bucket=bucket.name
            )

            if settings.destination.key_prefix:
                full_key = settings.destination.key_prefix + "." + text_type(synch.next_key) + ":" + text_type(MIN(g.get("_meta.count")))
            else:
                full_key = text_type(synch.next_key) + ":" + text_type(MIN(g.get("_meta.count")))
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
                                "name": coalesce(*settings.source.name),
                                "exchange": d._meta.exchange,
                                "id": d._meta.count,
                                "count": d._meta.count,
                                "message_id": d._meta.message_id,
                                "sent": Date(d._meta.sent),
                                "source": {
                                    "id": settings.destination.key_prefix
                                },
                                "type": "join"
                            },
                            "type": "aggregation"
                        }}
                    )
                    for i, d in enumerate(g)
                    if d != None  # HAPPENS WHEN PERSISTENT QUEUE FAILS TO LOG start
                ]
                bucket.write(full_key, "\n".join(value2json(d) for d in output))
                synch.advance()
                synch.source_key = MAX(g.get("_meta.count")) + 1

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
            except Exception as e:
                queue.rollback()
                if not queue.closed:
                    Log.warning("Problem writing {{key}} to S3", key=full_key, cause=e)

            if please_stop:
                break
    except Exception as e:
        Log.warning("Problem in the log loop", cause=e)
    finally:
        if work_queue != None:
            work_queue.close()
    Log.note("log_loop() IS DONE")


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        constants.set(settings.constants)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            with aws.s3.Bucket(settings.destination) as bucket:
                settings.source=listwrap(settings.source)

                if settings.param.debug:
                    if any(settings.source.durable):
                        Log.error("Can not run in debug mode with a durable queue")
                    synch = SynchState(bucket.get_key(SYNCHRONIZATION_KEY, must_exist=False))
                else:
                    synch = SynchState(bucket.get_key(SYNCHRONIZATION_KEY, must_exist=False))
                    if any(settings.source.durable):
                        synch.startup()

                queue = PersistentQueue(settings.param.queue_file)
                if queue:
                    last_item = queue[len(queue) - 1]
                    synch.source_key = last_item._meta.count + 1

                context = [
                    pulse.Consumer(kwargs=s, target=None, target_queue=queue, start=synch.source_key)
                    for s in settings.source
                ]

                with ExitStack(*context):
                    Thread.run("pulse log loop", log_loop, settings, synch, queue, bucket)
                    Thread.wait_for_shutdown_signal(allow_exit=True)
                    Log.warning("starting shutdown")

                queue.close()
                Log.note("write shutdown state to S3")
                synch.shutdown()

    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


class ExitStack(object):

    def __init__(self, *context):
        self.context=context

    def __enter__(self):
        for i, c in enumerate(self.context):
            try:
                c.__enter__()
            except Exception as e:
                e = Except.wrap(e)
                for ii in range(i):
                    try:
                        self.context[ii].__exit__(Except, e, None)
                    except Exception:
                        pass
                Log.error("problem entering context", cause=e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for c in self.context:
            try:
                c.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass


if __name__ == "__main__":
    main()


