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
from pyLibrary.aws.s3 import strip_extension
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.env import elasticsearch
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.thread.threads import Thread, Signal
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
        Log.note("Indexing {{key}}", {"key": key})
        with extend_time:
            num_keys = es.copy([key], bucket, {"terms": {"build.branch": settings.sample_only}} if settings.sample_only != None else None)

        if num_keys > 1:
            Log.note("Added {{num}} keys from {{key}} block in {{duration|round(places=2)}} seconds ({{rate|round(places=3)}} keys/second)", {
                "num": num_keys,
                "key": key,
                "duration": extend_time.seconds,
                "rate": num_keys / Math.max(extend_time.seconds, 0.01)
            })

        work_queue.commit()


def get_all_s3(in_es, settings):
    # EVERYTHING FROM S3
    bucket = s3.Bucket(settings.source)
    prefixes = [p.name.rstrip(":") for p in bucket.list(prefix="", delimiter=":")]
    in_s3 = []
    for i, p in enumerate(prefixes):
        if i % 1000 == 0:
            Log.note("Scrubbed {{p|percent(decimal=1)}}", {"p": i / len(prefixes)})
        try:
            if int(p) not in in_es:
                in_s3.append(int(p))
            else:
                pass
        except Exception, _:
            Log.note("delete key {{key}}", {"key": p})
            bucket.delete_key(strip_extension(p))
    in_s3 = qb.reverse(qb.sort(in_s3))
    return in_s3


def diff(settings, please_stop=None):
    # EVERYTHING FROM ELASTICSEARCH
    es = MultiDayIndex(settings.elasticsearch, queue_size=100000)

    in_es = get_all_in_es(es)
    in_s3 = get_all_s3(in_es, settings)

    Log.note("Queueing {{num}} keys for insertion to ES with {{threads}} threads", {
        "num": len(in_s3),
        "threads": settings.threads
    })
    # IGNORE THE 500 MOST RECENT BLOCKS, BECAUSE THEY ARE PROBABLY NOT DONE
    max_s3 = in_s3[0] - 500
    i = 0
    while in_s3[i] > max_s3:
        i += 1
    in_s3 = in_s3[i::]

    bucket = s3.Bucket(settings.source)
    work_queue = aws.Queue(settings=settings.work_queue)

    for block in in_s3:
        keys = [k.key for k in bucket.list(prefix=unicode(block) + ":")]
        work_queue.extend(keys)


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

        Log.note("got {{num}} from {{index}}", {
            "num": len(good_es),
            "index": name
        })
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
            Log.error("do not know how to handle")

        # diff(settings)
        work_queue = aws.Queue(settings=settings.work_queue)
        es = MultiDayIndex(settings.elasticsearch, queue_size=100000)

        threads = []
        please_stop = Signal()
        for _ in range(settings.threads):
            p = Thread.run("copy to es", copy2es, es, settings, work_queue, please_stop=please_stop)
            threads.append(p)

        def monitor_progress(please_stop):
            while not please_stop:
                Log.note("Remaining: {{num}}", {"num": len(work_queue)})
                Thread.sleep(seconds=10)

        Thread.run(name="monitor progress", target=monitor_progress)

        Thread.wait_for_shutdown_signal(please_stop=please_stop, allow_exit=True)
        please_stop.go()
        Log.note("Shutdown started")
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
