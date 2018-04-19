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

from collections import Mapping

from activedata_etl.etl import parse_id_argument
from jx_base import container
from mo_dots import coalesce, unwrap, Data, wrap
from mo_logs import Log, machine_metadata
from mo_logs import startup, constants
from mo_logs.exceptions import Explanation, WarnOnException
from mo_math import MAX
from mo_math.randoms import Random
from mo_threads import Process, Thread, Signal, Queue, THREAD_STOP, MAIN_THREAD
from mo_threads import Till
from mo_times.timer import Timer
from pyLibrary import aws
from pyLibrary.aws import s3
from pyLibrary.env import elasticsearch
from pyLibrary.env.rollover_index import RolloverIndex

split = {}
empty_bucket_complaint_sent = False


def splitter(work_queue, please_stop):
    global empty_bucket_complaint_sent

    for pair in iter(work_queue.pop_message, ""):
        if please_stop:
            for k, v in split.items():
                v.add(THREAD_STOP)
            return
        if pair == None:
            # ADD BACKFILLING HERE
            (Till(seconds=5) | please_stop).wait()
            continue

        message, payload = pair
        if not isinstance(payload, Mapping):
            Log.error("Not expecting a Mapping payload with `key` and `bucket` properties")

        key = payload.key
        with Explanation("Indexing records from {{bucket}}", bucket=payload.bucket):
            params = split.get(payload.bucket)
            if not params:
                message.delete()
                continue

        es = params.es
        source_bucket = params.bucket
        settings = params.settings

        extend_time = Timer("insert", silent=True)

        with extend_time:
            if settings.skip and Random.float() < settings.skip:
                Log.note("Skipping {{key}} from bucket {{bucket}}", key=key, bucket=source_bucket.name)
                work_queue.add(payload)
                message.delete()
                continue

            if settings.sample_only:
                sample_filter = {"terms": {"build.branch": settings.sample_only}}
            elif settings.sample_size:
                sample_filter = True
            else:
                sample_filter = None

            Log.note("Indexing {{key}} from bucket {{bucket}}", key=key, bucket=source_bucket.name)
            more_keys = source_bucket.keys(prefix=key)
            if not more_keys:
                # HAPPENS WHEN REPROCESSING (ETL WOULD HAVE CLEARED THE BUCKET OF THIS PREFIX FIRST)
                if not empty_bucket_complaint_sent:
                    empty_bucket_complaint_sent = True
                    Log.warning("No files found in bucket {{message|json}}. THIS WARNING WILL NOT BE SENT AGAIN!!", message=payload)
                message.delete()
                num_keys = 0
            else:
                num_keys = es.copy(more_keys, source_bucket, sample_filter, settings.sample_size, message.delete)

        if num_keys > 1:
            Log.note(
                "Added {{num}} keys from {{key}} in {{bucket}} to {{es}} in {{duration}} ({{rate|round(places=3)}} keys/second)",
                num=num_keys,
                key=key,
                bucket=source_bucket.name,
                es=es.settings.index,
                duration=extend_time.duration,
                rate=num_keys / MAX([extend_time.duration.seconds, 0.01])
            )


def safe_splitter(work_queue, please_stop):
    while not please_stop:
        try:
            with WarnOnException("Indexing records"):
                splitter(work_queue, please_stop)
        except Exception as e:
            Log.warning("problem", cause=e)


def add_message_confirmation(queue, payload_key, message):
    def _delete():
        # Log.note("confirming message for {{id}}", id=payload_key)
        message.delete()

    queue.add(_delete)


def shutdown_local_es_node():
    Log.warning("Shutdown ES on node {{node}}", node=machine_metadata)
    with Process("stop es", ["sudo", "supervisorctl", "stop", "es"]) as proc:
        while True:
            line = proc.stdout.pop().strip()
            if not line:
                continue
            if line == THREAD_STOP:
                break
            Log.note("Shutdown es: {{note}}", note=line)


def main():
    try:
        settings = startup.read_settings(defs=[
            {
                "name": ["--id", "--key"],
                "help": "id to process (prefix is ok too) ",
                "type": str,
                "dest": "id",
                "required": False
            },
            {
                "name": ["--new", "--reset"],
                "help": "to make a new index (then exit immediately)",
                "type": str,
                "dest": "reset",
                "required": False
            }
        ])
        constants.set(settings.constants)
        Log.start(settings.debug)

        container.config.default = {
            "type": "elasticsearch",
            "settings": settings.elasticsearch.copy()
        }

        if settings.args.reset:
            es_settings = wrap([w.elasticsearch for w in settings.workers if w.name == settings.args.reset])
            if not es_settings:
                Log.error("Can not find worker going by name {{name|quote}}", name=settings.args.reset)
            elif len(es_settings) > 1:
                Log.error("More than one worker going by name {{name|quote}}", name=settings.args.reset)
            else:
                es_settings = es_settings.last()

            cluster = elasticsearch.Cluster(es_settings)
            alias = coalesce(es_settings.alias, es_settings.index)
            index = cluster.get_prototype(alias)[0]
            if index:
                Log.error("Index {{index}} has prefix={{alias|quote}}, and has no alias.  Can not make another.", alias=alias, index=index)
            else:
                Log.alert("Creating index for alias={{alias}}", alias=alias)
                cluster.create_index(kwargs=es_settings)
                Log.alert("Done.  Exiting.")
                return

        if settings.args.id:
            main_work_queue = Queue("local work queue")
            for w in settings.workers:
                bucket = s3.Bucket(w.source)
                for prefixes in parse_id_argument(settings.args.id):
                    keys = bucket.keys(prefix=prefixes)
                    for k in keys:
                        main_work_queue.add(Data(
                            key=k,
                            bucket=bucket.name
                        ))
        else:
            main_work_queue = aws.Queue(kwargs=settings.work_queue)
        Log.note("Listen to queue {{queue}}, and read off of {{s3}}", queue=settings.work_queue.name, s3=settings.workers.source.bucket)

        for w in settings.workers:
            if not w.rollover.interval or not w.rollover.field:
                Log.error("All workers must declare an `rollover.interval` which will indicate when to rollover to a fresh index")

            split[w.source.bucket] = Data(
                es=RolloverIndex(
                    rollover_field=w.rollover.field,
                    rollover_interval=w.rollover.interval,
                    rollover_max=w.rollover.max,
                    queue_size=coalesce(w.queue_size, 1000),
                    batch_size=unwrap(w.batch_size),
                    kwargs=w.elasticsearch
                ),
                bucket=s3.Bucket(w.source),
                settings=w
            )
            Log.note("Bucket {{bucket}} pushed to ES {{index}}", bucket=w.source.bucket, index=split[w.source.bucket].es.settings.index)

        please_stop = Signal()
        aws_shutdown = Signal("aws shutdown")
        aws_shutdown.on_go(shutdown_local_es_node)
        aws_shutdown.on_go(please_stop.go)
        aws.capture_termination_signal(aws_shutdown)

        Thread.run("splitter", safe_splitter, main_work_queue, please_stop=please_stop)

        def monitor_progress(please_stop):
            while not please_stop:
                Log.note("Remaining in SQS: {{num}}", num=len(main_work_queue))
                (please_stop | Till(seconds=10)).wait()

        Thread.run(name="monitor progress", target=monitor_progress, please_stop=please_stop)

        MAIN_THREAD.wait_for_shutdown_signal(please_stop=please_stop, allow_exit=True)
        please_stop.go()
        Log.note("Shutdown")
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
