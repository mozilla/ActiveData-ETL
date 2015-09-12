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

from pyLibrary import queries, aws
from pyLibrary.aws import s3
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce
from pyLibrary.env import elasticsearch
from pyLibrary.maths import Math
from pyLibrary.thread.threads import Thread, Signal, Queue
from pyLibrary.times.timer import Timer
from testlog_etl.etl import parse_id_argument
from testlog_etl.sinks.multi_day_index import MultiDayIndex


# COPY FROM S3 BUCKET TO ELASTICSEARCH
def copy2es(es, settings, work_queue, please_stop=None):
    # EVERYTHING FROM ELASTICSEARCH
    bucket = s3.Bucket(settings.source)

    for key in iter(work_queue.pop, ""):
        if please_stop:
            return
        if key == None:
            continue

        key = unicode(key)
        extend_time = Timer("insert", silent=True)
        Log.note("Indexing {{key}}", key=key)
        with extend_time:
            if settings.sample_only:
                sample_filter = {"terms": {"build.branch": settings.sample_only}}
            elif settings.sample_size:
                sample_filter = True
            else:
                sample_filter = None

            if key.find(":")>=0:
                more_keys = bucket.keys(prefix=key)
            else:
                more_keys = bucket.keys(prefix=key + ":")
            num_keys = es.copy(more_keys, bucket, sample_filter, settings.sample_size)

        if num_keys > 1:
            Log.note(
                "Added {{num}} keys from {{key}} block in {{duration}} ({{rate|round(places=3)}} keys/second)",
                num=num_keys,
                key=key,
                duration=extend_time.duration,
                rate=num_keys / Math.max(extend_time.duration.seconds, 0.01)
            )

        work_queue.commit()


def main():
    try:
        settings = startup.read_settings(defs=[
            {
                "name": ["--id"],
                "help": "id to process (prefix is ok too) ",
                "type": str,
                "dest": "id",
                "required": False
            },
            {
                "name": ["--new", "--reset"],
                "help": "to make a new index (exit immediately)",
                "action": 'store_true',
                "dest": "reset",
                "required": False
            }
        ])
        constants.set(settings.constants)
        Log.start(settings.debug)

        queries.config.default = {
            "type": "elasticsearch",
            "settings": settings.elasticsearch.copy()
        }

        if settings.args.reset:
            c = elasticsearch.Cluster(settings.elasticsearch)
            alias = coalesce(settings.elasticsearch.alias, settings.elasticsearch.index)
            index = c.get_prototype(alias)[0]
            if index:
                Log.error("Index {{index}} has prefix={{alias|quote}}, and has no alias.  Can not make another.", alias=alias, index=index)
            else:
                Log.alert("Creating index for alias={{alias}}", alias=alias)
                c.create_index(settings=settings.elasticsearch)
                Log.alert("Done.  Exiting.")
                return

        if settings.args.id:
            work_queue = Queue("local work queue")
            work_queue.extend(parse_id_argument(settings.args.id))
        else:
            work_queue = aws.Queue(settings=settings.work_queue)

        Log.note("Listen to queue {{queue}}, and read off of {{s3}}", queue=settings.work_queue.name, s3=settings.source.bucket)

        es = MultiDayIndex(settings.elasticsearch, queue_size=100000)

        threads = []
        please_stop = Signal()
        for _ in range(settings.threads):
            p = Thread.run("copy to es", copy2es, es, settings, work_queue, please_stop=please_stop)
            threads.append(p)

        def monitor_progress(please_stop):
            while not please_stop:
                Log.note("Remaining: {{num}}", num=len(work_queue))
                Thread.sleep(seconds=10)

        Thread.run(name="monitor progress", target=monitor_progress, please_stop=please_stop)

        aws.capture_termination_signal(please_stop)
        Thread.wait_for_shutdown_signal(please_stop=please_stop, allow_exit=True)
        please_stop.go()
        Log.note("Shutdown started")
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
