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

from collections import Mapping

from pyLibrary import queries, aws
from pyLibrary.aws import s3
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, unwrap, Dict
from pyLibrary.env import elasticsearch
from pyLibrary.maths import Math
from pyLibrary.thread.threads import Thread, Signal, Queue
from pyLibrary.times.timer import Timer
from testlog_etl.etl import parse_id_argument
from testlog_etl.sinks.multi_day_index import MultiDayIndex

split = {}


def splitter(work_queue, please_stop):
    for pair in iter(work_queue.pop_message, ""):
        if please_stop:
            for k,v in split.items():
                v.add(Thread.STOP)
            return
        if pair == None:
            continue

        message, payload = pair
        if not isinstance(payload, Mapping):
            Log.error("not expected")

        key = payload.key
        try:
            params = split[payload.bucket]
        except Exception:
            Log.error("do not know what to do with bucket {{bucket}}", bucket=payload.bucket)
        es = params.es
        bucket = params.bucket
        settings = params.settings

        extend_time = Timer("insert", silent=True)
        Log.note("Indexing {{key}} from bucket {{bucket}}", key=key, bucket=bucket.name)
        with extend_time:
            if settings.sample_only:
                sample_filter = {"terms": {"build.branch": settings.sample_only}}
            elif settings.sample_size:
                sample_filter = True
            else:
                sample_filter = None

            more_keys = bucket.keys(prefix=key)
            num_keys = es.copy(more_keys, bucket, sample_filter, settings.sample_size)

            def _delete():
                Log.note("confirming message for {{id}}", id=payload.key)
                message.delete()

            es.queue.add(_delete)

        if num_keys > 1:
            Log.note(
                "Added {{num}} keys from {{key}} block to {{bucket}} in {{duration}} ({{rate|round(places=3)}} keys/second)",
                num=num_keys,
                key=key,
                bucket=bucket.name,
                duration=extend_time.duration,
                rate=num_keys / Math.max(extend_time.duration.seconds, 0.01)
            )


def safe_splitter(work_queue, please_stop):
    try:
        splitter(work_queue, please_stop)
    finally:
        please_stop.go()


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
            Log.error("not working, multiple indexes involved")
            cluster = elasticsearch.Cluster(settings.elasticsearch)
            alias = coalesce(settings.elasticsearch.alias, settings.elasticsearch.index)
            index = cluster.get_prototype(alias)[0]
            if index:
                Log.error("Index {{index}} has prefix={{alias|quote}}, and has no alias.  Can not make another.", alias=alias, index=index)
            else:
                Log.alert("Creating index for alias={{alias}}", alias=alias)
                cluster.create_index(settings=settings.elasticsearch)
                Log.alert("Done.  Exiting.")
                return

        if settings.args.id:
            main_work_queue = Queue("local work queue")
            main_work_queue.extend(parse_id_argument(settings.args.id))
        else:
            main_work_queue = aws.Queue(settings=settings.work_queue)
        Log.note("Listen to queue {{queue}}, and read off of {{s3}}", queue=settings.work_queue.name, s3=settings.workers.source.bucket)

        for w in settings.workers:
            split[w.source.bucket] = Dict(
                es=MultiDayIndex(w.elasticsearch, queue_size=coalesce(w.queue_size, 1000), batch_size=unwrap(w.batch_size)),
                bucket=s3.Bucket(w.source),
                settings=settings
            )

        please_stop=Signal()
        Thread.run("splitter", safe_splitter, main_work_queue, please_stop=please_stop)

        def monitor_progress(please_stop):
            while not please_stop:
                Log.note("Remaining: {{num}}", num=len(main_work_queue))
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
