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
from pyLibrary.env import elasticsearch
from pyLibrary.maths import Math
from pyLibrary.thread.threads import Thread, Signal, Queue
from pyLibrary.times.timer import Timer
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

        extend_time = Timer("insert", silent=True)
        Log.note("Indexing {{key}}", key=key)
        with extend_time:
            if settings.sample_only:
                sample_filter = {"terms": {"build.branch": settings.sample_only}}
            elif settings.sample_size:
                sample_filter = True
            else:
                sample_filter = None

            num_keys = es.copy([key], bucket, sample_filter, settings.sample_size)

        if num_keys > 1:
            Log.note(
                "Added {{num}} keys from {{key}} block in {{duration}} ({{rate|round(places=3)}} keys/second)",
                num=num_keys,
                key=key,
                duration=extend_time.duration,
                rate=num_keys / Math.max(extend_time.duration.seconds, 0.01)
            )

        work_queue.commit()


def get_all_in_es(es):
    in_es = set()

    all_indexes = es.es.cluster.get_metadata().indices
    for name, index in all_indexes.items():
        if "unittest" not in index.aliases:
            continue

        result = elasticsearch.Index(index=name, alias="unittest", settings=es.es.settings).search({
            "aggs": {
                "_match": {
                    "terms": {
                        "field": "etl.source.source.id",
                        "size": 200000
                    }

                }
            }
        })

        good_es = []
        for k in result.aggregations._match.buckets.key:
            try:
                good_es.append(int(k))
            except Exception, e:
                pass

        Log.note("got {{num}} from {{index}}",
            num= len(good_es),
            index= name)
        in_es |= set(good_es)

    return in_es


def main():
    try:
        settings = startup.read_settings(defs=[
            {
                "name": ["--id"],
                "help": "id (prefix, really) to process",
                "type": str,
                "dest": "id",
                "required": False
            }
        ])
        constants.set(settings.constants)
        Log.start(settings.debug)

        queries.config.default = {
            "type": "elasticsearch",
            "settings": settings.elasticsearch.copy()
        }

        if settings.args.id:
            work_queue = Queue("local work queue")
            work_queue.add(settings.args.id)
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
