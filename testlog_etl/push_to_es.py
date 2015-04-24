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
from multiprocessing import Process
from multiprocessing.queues import Queue

from pyLibrary import queries
from pyLibrary.aws import s3
from pyLibrary.aws.s3 import strip_extension
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.jsons import scrub
from pyLibrary.queries import qb
from pyLibrary.thread.threads import Thread
from testlog_etl.push_to_es_daemon import copy2es
from testlog_etl.sinks.multi_day_index import MultiDayIndex




# COPY FROM S3 BUCKET TO ELASTICSEARCH

def diff(settings, please_stop=None):
    # EVERYTHING FROM ELASTICSEARCH
    es = MultiDayIndex(settings.elasticsearch, queue_size=100000)

    result = es.search({
        "aggs": {
            "_match": {
                "terms": {
                    "field": "etl.source.source.id",
                    "size": 0
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
    in_es = set(good_es)

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

    Log.note("Queueing {{num}} keys for insertion to ES with {{processes}}", {
        "num": len(in_s3),
        "processes": settings.processes
    })
    # IGNORE THE 500 MOST RECENT BLOCKS, BECAUSE THEY ARE PROBABLY NOT DONE
    max_s3 = in_s3[0] - 500
    i = 0
    while in_s3[i] > max_s3:
        i += 1
    in_s3 = in_s3[i::]

    # DISPATCH TO MULTIPLE THREADS
    work_queue = Queue()
    please_stop_queue = Queue()

    processes = []
    for _ in range(settings.processes):
        p = Process(target=copy2es, args=(scrub(settings), work_queue, please_stop_queue))
        p.start()
        processes.append(p)

    for block in in_s3:
        work_queue.put(block)
    for _ in range(settings.processes):
        work_queue.put("STOP")

    size = work_queue.qsize()
    while not please_stop and size:
        Log.note("Remaining: {{num}}", {"num": size})
        Thread.sleep(seconds=5)
        size = work_queue.qsize()

    for _ in range(settings.processes):
        please_stop_queue.put("STOP")
    for p in processes:
        p.join()


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

        thread = Thread.run("pushing to es", diff, settings)
        Thread.wait_for_shutdown_signal(thread.please_stop, allow_exit=True)

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
