# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals


# import cProfile
# import pstats
#
# cprofiler = cProfile.Profile()
# cprofiler.enable()
#

# NEED TO BE NOTIFIED OF ID TO REPROCESS
# NEED TO BE NOTIFIED OF RANGE TO REPROCESS
# MUST SEND CONSEQUENCE DOWN THE STREAM SO OTHERS CAN WORK ON IT
from collections import Mapping
from copy import deepcopy
import sys

from mo_kwargs import override

import mo_dots
from pyLibrary import aws
from pyLibrary.aws.s3 import strip_extension, key_prefix, KEY_IS_WRONG_FORMAT
from mo_math import MIN
from mo_logs import Log, startup, constants, strings
from mo_logs.exceptions import suppress_exception
from mo_dots import coalesce, listwrap, Data, Null, wrap
from pyLibrary.env import elasticsearch
from pyLibrary.env.rollover_index import RolloverIndex
from pyLibrary.queries import jx
from mo_testing import fuzzytestcase
from mo_threads import Thread, Signal, Queue, Lock
from mo_threads import Till
from mo_times.dates import Date
from mo_times.durations import SECOND
from activedata_etl import key2etl
from mohg.hg_mozilla_org import HgMozillaOrg
from activedata_etl.sinks.dummy_sink import DummySink
from activedata_etl.sinks.s3_bucket import S3Bucket
from activedata_etl.sinks.split import Split
from activedata_etl.transforms import Transform

from pyLibrary import convert

EXTRA_WAIT_TIME = 20 * SECOND  # WAIT TIME TO SEND TO AWS, IF WE wait_forever


class ConcatSources(object):
    """
    MAKE MANY SOURCES LOOK LIKE ONE
    """

    def __init__(self, sources):
        self.source = sources

    def read(self):
        return "\n".join(s.read() for s in self.sources)


class ETL(Thread):
    @override
    def __init__(
        self,
        name,
        work_queue,
        workers,
        resources,
        please_stop,
        wait_forever=False,
        kwargs=None
    ):
        # FIND THE WORKERS METHODS
        kwargs.workers = []
        for w in workers:
            w = deepcopy(w)

            for existing_worker in kwargs.workers:
                try:
                    fuzzytestcase.assertAlmostEqual(existing_worker.source, w.source)
                    fuzzytestcase.assertAlmostEqual(existing_worker.transformer, w.transformer)
                    # SAME SOURCE AND TRANSFORMER, MERGE THE destinations
                except Exception as e:
                    continue
                destination = get_container(w.destination)
                existing_worker._destination = Split(existing_worker._destination, destination)
                break
            else:
                t_name = w.transformer
                w._transformer = mo_dots.get_attr(sys.modules, t_name)
                if not w._transformer:
                    Log.error("Can not find {{path}} to transformer (are you sure you are pointing to a function?  Do you have all dependencies?)", path=t_name)
                elif isinstance(w._transformer, object.__class__) and issubclass(w._transformer, Transform):
                    # WE EXPECT A FUNCTION.  THE Transform INSTANCES ARE, AT LEAST, CALLABLE
                    w._transformer = w._transformer.__new__()
                w._source = get_container(w.source)
                w._destination = get_container(w.destination)
                kwargs.workers.append(w)

            w._notify = []
            for notify in listwrap(w.notify):
                w._notify.append(aws.Queue(notify))

        self.resources = resources
        self.settings = kwargs
        if isinstance(work_queue, Mapping):
            self.work_queue = aws.Queue(work_queue)
        else:
            self.work_queue = work_queue

        Thread.__init__(self, name, self.loop, please_stop=please_stop)
        Log.note("--- finished ETL transform thread ---")
        self.start()

    def _dispatch_work(self, source_block):
        """
        source_block POINTS TO THE bucket AND key TO PROCESS
        :return: False IF THERE IS NOTHING LEFT TO DO
        """
        source_keys = listwrap(coalesce(source_block.key, source_block.keys))

        if not isinstance(source_block.bucket, basestring):  # FIX MISTAKE
            source_block.bucket = source_block.bucket.bucket
        bucket = source_block.bucket

        if source_block.destination:
            # EXTRA FILTER BY destination
            work_actions = [w for w in self.settings.workers if w.source.bucket == bucket and w.destination.bucket == source_block.destination]
        else:
            work_actions = [w for w in self.settings.workers if w.source.bucket == bucket]

        if not work_actions:
            Log.note(
                "No worker defined for records from {{source_bucket|quote}} to {{destination|quote}}, {{action}}.\n{{message|indent}}",
                source_bucket=source_block.bucket,
                destination=source_block.destination,
                message=source_block,
                action="skipping" if self.settings.keep_unknown_on_queue else "deleting"
            )
            return not self.settings.keep_unknown_on_queue

        for action in work_actions:
            try:
                source_key = unicode(source_keys[0])
                if len(source_keys) > 1:
                    multi_source = action._source
                    source = ConcatSources([multi_source.get_key(k) for k in source_keys])
                    source_key = MIN(source_key)
                else:
                    source = action._source.get_key(source_key)
                    source_key = source.key

                Log.note(
                    "Execute {{action}} on bucket={{source}} key={{key}} to destination={{actt}}",
                    action=action.name,
                    source=source_block.bucket,
                    key=source_key,
                    actt=action._destination.bucket.name
                )

                if action.transform_type == "bulk":
                    old_keys = set()
                else:
                    old_keys = action._destination.keys(prefix=source_block.key)

                new_keys = set(action._transformer(source_key, source, action._destination, resources=self.resources, please_stop=self.please_stop))

                if not new_keys and old_keys:
                    Log.warning(
                        "Expecting some new keys after etl of {{source_key}}, especially since there were old ones\n{{old_keys}}",
                        old_keys=old_keys,
                        source_key=source_key
                    )
                    continue
                elif new_keys == None:
                    new_keys = set()
                elif len(new_keys) == 0:
                    Log.alert(
                        "Expecting some new keys after processing {{source_key}}",
                        old_keys=old_keys,
                        source_key=source_key
                    )
                    continue

                # VERIFY KEYS
                etls = map(key2etl, new_keys)
                etl_ids = jx.sort(set(wrap(etls).id))
                if len(new_keys) == 1 and list(new_keys)[0].endswith(source_key):
                    pass  # ok
                elif len(etl_ids) == 1 and key2etl(source_key).id==etl_ids[0]:
                    pass  # ok
                else:
                    for i, eid in enumerate(etl_ids):
                        if i != eid:
                            Log.error("expecting keys to be contiguous: {{ids}}", ids=etl_ids)
                    # VERIFY KEYS EXIST
                    if hasattr(action._destination, "get_key"):
                        for k in new_keys:
                            action._destination.get_key(k)

                for n in action._notify:
                    for k in new_keys:
                        now = Date.now()
                        n.add({
                            "bucket": action._destination.bucket.name,
                            "key": k,
                            "timestamp": now.unix,
                            "date/time": now.format()
                        })

                if action.transform_type == "bulk":
                    continue

                for n in new_keys:
                    if not n.startswith(source_key):
                        Log.error("Expecting new keys ({{new_key}}) to start with source key ({{source_key}})",  new_key= n,  source_key= source_key)

                delete_me = old_keys - new_keys
                if delete_me:
                    if action.destination.bucket == "ekyle-test-result":
                        for k in delete_me:
                            action._destination.delete_key(k)
                    else:
                        Log.note("delete keys?\n{{list}}", list=sorted(delete_me))
                        # for k in delete_me:
                # WE DO NOT PUT KEYS ON WORK QUEUE IF ALREADY NOTIFYING SOME OTHER
                # AND NOT GOING TO AN S3 BUCKET
                if not action._notify and isinstance(action._destination, (aws.s3.Bucket, S3Bucket)):
                    for k in old_keys | new_keys:
                        now = Date.now()
                        self.work_queue.add({
                            "bucket": action.destination.bucket,
                            "key": k,
                            "timestamp": now.unix,
                            "date/time": now.format()
                        })
            except Exception as e:
                if "Key {{key}} does not exist" in e:
                    err = Log.warning
                elif "multiple keys in {{bucket}}" in e:
                    err = Log.warning
                    if source_block.bucket=="ekyle-test-result":
                        for k in action._source.list(prefix=key_prefix(source_key)):
                            action._source.delete_key(strip_extension(k.key))
                elif "expecting keys to be contiguous" in e:
                    err = Log.warning
                elif "Expecting a pure key" in e:
                    err = Log.warning
                elif KEY_IS_WRONG_FORMAT in e:
                    err = Log.warning
                else:
                    err = Log.error

                err(
                    "Problem transforming {{action}} on bucket={{source}} key={{key}} to destination={{destination}}",
                    action=action.name,
                    source=source_block.bucket,
                    key=source_key,
                    destination=coalesce(action.destination.name, action.destination.index),
                    cause=e
                )
        return True

    def loop(self, please_stop):
        try:
            with self.work_queue:
                while not please_stop:
                    if self.settings.wait_forever:
                        todo = None
                        while not please_stop and not todo:
                            if isinstance(self.work_queue, aws.Queue):
                                todo = self.work_queue.pop(wait=EXTRA_WAIT_TIME)
                            else:
                                todo = self.work_queue.pop()
                        if not todo:
                            break  # please_stop MUST HAVE BEEN TRIGGERED

                    else:
                        if isinstance(self.work_queue, aws.Queue):
                            todo = self.work_queue.pop()
                        else:
                            todo = self.work_queue.pop(till=Till(till=Date.now().unix))
                        if todo == None:
                            please_stop.go()
                            return

                    if todo == None:
                        Log.warning("Should never happen")
                        continue

                    if isinstance(todo, unicode):
                        Log.warning("Work queue had {{data|json}}, which is not valid", data=todo)
                        self.work_queue.commit()
                        continue


                    try:
                        is_ok = self._dispatch_work(todo)
                        if is_ok:
                            self.work_queue.commit()
                        else:
                            self.work_queue.rollback()
                    except Exception as e:
                        # WE CERTAINLY EXPECT TO GET HERE IF SHUTDOWN IS DETECTED, NO NEED TO TELL
                        if "Shutdown detected." in e:
                            continue

                        previous_attempts = coalesce(todo.previous_attempts, 0)
                        todo.previous_attempts = previous_attempts + 1

                        if previous_attempts < coalesce(self.settings.min_attempts, 3):
                            # SILENT
                            try:
                                self.work_queue.add(todo)
                                self.work_queue.commit()
                            except Exception, f:
                                # UNEXPECTED PROBLEM!!!
                                self.work_queue.rollback()
                                Log.warning("Could not annotate todo", cause=[f, e])
                        elif previous_attempts > 10:
                            #GIVE UP
                            Log.warning(
                                "After {{tries}} attempts, still could not process {{key}}.  ***REJECTED***",
                                tries=todo.previous_attempts,
                                key=todo.key,
                                cause=e
                            )
                            self.work_queue.commit()
                        else:
                            # COMPLAIN
                            try:
                                self.work_queue.add(todo)
                                self.work_queue.commit()
                            except Exception, f:
                                # UNEXPECTED PROBLEM!!!
                                self.work_queue.rollback()
                                Log.warning("Could not annotate todo", cause=[f, e])

                            Log.warning(
                                "After {{tries}} attempts, still could not process {{key}}.  Returned back to work queue.",
                                tries=todo.previous_attempts,
                                key=todo.key,
                                cause=e
                            )
        except Exception as e:
            Log.warning("Failure in the ETL loop", cause=e)
            raise e

sinks_locker = Lock()
sinks = []  # LIST OF (settings, sink) PAIRS


def get_container(settings):
    if isinstance(settings, (RolloverIndex, aws.s3.Bucket)):
        return settings

    if settings == None:
        return DummySink()
    elif settings.type == "redshift":
        Log.error("not supported, removed oct2015")
    elif coalesce(settings.aws_access_key_id, settings.aws_access_key_id, settings.region):
        # ASSUME BUCKET NAME
        with sinks_locker:
            for e in sinks:
                with suppress_exception:
                    fuzzytestcase.assertAlmostEqual(e[0], settings)
                    return e[1]
            output =  S3Bucket(settings)
            sinks.append((settings, output))
            return output
    else:
        with sinks_locker:
            for e in sinks:
                with suppress_exception:
                    fuzzytestcase.assertAlmostEqual(e[0], settings)
                    return e[1]


            es = elasticsearch.Cluster(kwargs=settings).get_or_create_index(kwargs=settings)
            output = es.threaded_queue(max_size=2000, batch_size=1000)
            setattr(output, "keys", lambda prefix: set())

            sinks.append((settings, output))
            return output

# ToDo = DataClass("ToDo", [
#     {
#         # THE KEY(S) TO USE
#         "name": "keys",
#         "required": True,
#         "nulls": False
#     },
#     {
#         # THE SOURCE BUCKET
#         "name": "bucket",
#         "required": True,
#         "nulls": False
#     },
#     {
#         # OPTIONAL DESTINATION, TO LIMIT THE ACTIONS TAKEN (USUALLY USED FOR BACK FILLING SPECIFIC DATA)
#         "name": "detination",
#         "required": False
#     }
#
# ])


def main():

    try:
        settings = startup.read_settings(defs=[
            {
                "name": ["--id", "--key"],
                "help": "id(s) to process.  Use \"..\" for a range.",
                "type": str,
                "dest": "id",
                "required": False
            }
        ])
        constants.set(settings.constants)
        Log.start(settings.debug)

        if settings.args.id:
            etl_one(settings)
            return

        hg = HgMozillaOrg(use_cache=True, kwargs=settings.hg)
        resources = Data(
            hg=hg,
            local_es_node=settings.local_es_node
        )

        stopper = Signal()
        for i in range(coalesce(settings.param.threads, 1)):
            ETL(
                name="ETL Loop " + unicode(i),
                work_queue=settings.work_queue,
                resources=resources,
                workers=settings.workers,
                kwargs=settings.param,
                please_stop=stopper
            )

        aws.capture_termination_signal(stopper)
        Thread.wait_for_shutdown_signal(stopper, allow_exit=True)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()
        # write_profile(Data(filename="startup.tab"), [pstats.Stats(cprofiler)])


def etl_one(settings):
    queue = Queue("temp work queue")
    queue.__setattr__(b"commit", Null)
    queue.__setattr__(b"rollback", Null)

    settings.param.wait_forever = False
    already_in_queue = set()
    for w in settings.workers:
        source = get_container(w.source)
        # source.settings.fast_forward = True
        if id(source) in already_in_queue:
            continue
        try:
            for i in parse_id_argument(settings.args.id):
                keys = source.keys(i)
                for k in keys:
                    already_in_queue.add(k)
                    queue.add(Data(
                        bucket=w.source.bucket,
                        key=k
                    ))
        except Exception as e:
            if "Key {{key}} does not exist" in e:
                already_in_queue.add(id(source))
                queue.add(Data(
                    bucket=w.source.bucket,
                    key=settings.args.id
                ))
            Log.warning("Problem", cause=e)

    hg = HgMozillaOrg(kwargs=settings.hg)
    resources = Data(
        hg=hg,
        local_es_node=settings.local_es_node
    )

    stopper = Signal("main stop signal")
    ETL(
        name="ETL Loop Test",
        work_queue=queue,
        workers=settings.workers,
        kwargs=settings.param,
        resources=resources,
        please_stop=stopper
    )
    Thread.wait_for_shutdown_signal(stopper, allow_exit=True)


def parse_id_argument(id):
    many = map(strings.trim, id.split(","))
    if len(many) > 1:
        return many
    if id.find("..") >= 0:
        #range of ids
        min_, max_ = map(int, map(strings.trim, id.split("..")))
        return map(unicode, range(min_, max_ + 1))
    else:
        return [id]


if __name__ == "__main__":
    main()


