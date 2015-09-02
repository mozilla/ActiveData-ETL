# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

# NEED TO BE NOTIFIED OF ID TO REPROCESS
# NEED TO BE NOTIFIED OF RANGE TO REPROCESS
# MUST SEND CONSEQUENCE DOWN THE STREAM SO OTHERS CAN WORK ON IT
from collections import Mapping
from copy import deepcopy
import sys

from pyLibrary import aws, dot, strings
from pyLibrary.aws.s3 import strip_extension, key_prefix
from pyLibrary.collections import MIN
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, listwrap, Dict, Null
from pyLibrary.dot.objects import dictwrap
from pyLibrary.env import elasticsearch
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.testing import fuzzytestcase
from pyLibrary.thread.threads import Thread, Signal, Queue, Lock
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import Duration
from testlog_etl import key2etl
from testlog_etl.imports.hg_mozilla_org import HgMozillaOrg
from testlog_etl.sinks.dummy_sink import DummySink
from testlog_etl.sinks.multi_day_index import MultiDayIndex
from testlog_etl.sinks.redshift import Json2Redshift
from testlog_etl.sinks.s3_bucket import S3Bucket
from testlog_etl.sinks.split import Split


EXTRA_WAIT_TIME = 20 * Duration.SECOND  # WAIT TIME TO SEND TO AWS, IF WE wait_forever


class ConcatSources(object):
    """
    MAKE MANY SOURCES LOOK LIKE ONE
    """

    def __init__(self, sources):
        self.source = sources

    def read(self):
        return "\n".join(s.read() for s in self.sources)


class ETL(Thread):
    @use_settings
    def __init__(
        self,
        name,
        work_queue,
        workers,
        resources,
        please_stop,
        wait_forever=False,
        settings=None
    ):
        # FIND THE WORKERS METHODS
        settings.workers = []
        for w in workers:
            w = deepcopy(w)

            for existing_worker in settings.workers:
                try:
                    fuzzytestcase.assertAlmostEqual(existing_worker.source, w.source)
                    fuzzytestcase.assertAlmostEqual(existing_worker.transformer, w.transformer)
                    # SAME SOURCE AND TRANSFORMER, MERGE THE destinations
                except Exception, e:
                    continue
                destination = get_container(w.destination)
                existing_worker._destination = Split(existing_worker._destination, destination)
                break
            else:
                t_name = w.transformer
                w._transformer = dot.get_attr(sys.modules, t_name)
                if not w._transformer:
                    Log.error("Can not find {{path}} to transformer (are you sure you are pointing to a function?)", path=t_name)
                w._source = get_container(w.source)
                w._destination = get_container(w.destination)
                settings.workers.append(w)

            w._notify = []
            for notify in listwrap(w.notify):
                w._notify.append(aws.Queue(notify))

        self.resources = resources
        self.settings = settings
        if isinstance(work_queue, Mapping):
            self.work_queue = aws.Queue(work_queue)
        else:
            self.work_queue = work_queue

        Thread.__init__(self, name, self.loop, please_stop=please_stop)
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
        work_actions = [w for w in self.settings.workers if w.source.bucket == bucket]

        if not work_actions:
            Log.note(
                "No worker defined for records from {{bucket}}, {{action}}.\n{{message|indent}}",
                bucket=source_block.bucket,
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
                    "Execute {{action}} on bucket={{source}} key={{key}}",
                    action=action.name,
                    source=source_block.bucket,
                    key=source_key
                )

                if action.transform_type == "bulk":
                    old_keys = set()
                else:
                    old_keys = action._destination.keys(prefix=source_block.key)

                new_keys = set(action._transformer(source_key, source, action._destination, resources=self.resources, please_stop=self.please_stop))

                #VERIFY KEYS
                if len(new_keys) == 1 and list(new_keys)[0] == source_key:
                    pass  # ok
                else:
                    etls = map(key2etl, new_keys)
                    etls = qb.sort(etls, "id")
                    for i, e in enumerate(etls):
                        if i != e.id:
                            Log.error("expecting keys to have dense order: {{ids}}", ids=etls.id)
                    #VERIFY KEYS EXIST
                    if hasattr(action._destination, "get_key"):
                        for k in new_keys:
                            action._destination.get_key(k)

                for n in action._notify:
                    for k in new_keys:
                        n.add(k)

                if action.transform_type == "bulk":
                    continue

                # DUE TO BUGS THIS INVARIANT IS NOW BROKEN
                # TODO: FIGURE OUT HOW TO FIX THIS (CHANGE NAME OF THE SOURCE BLOCK KEY?)
                # for n in new_keys:
                #     if not n.startswith(source_key):
                #         Log.error("Expecting new keys ({{new_key}}) to start with source key ({{source_key}})",  new_key= n,  source_key= source_key)

                if not new_keys and old_keys:
                    Log.alert("Expecting some new keys after etl of {{source_key}}, especially since there were old ones\n{{old_keys}}",
                        old_keys= old_keys,
                        source_key= source_key)
                    continue
                elif not new_keys:
                    Log.alert(
                        "Expecting some new keys after processing {{source_key}}",
                        old_keys=old_keys,
                        source_key=source_key
                    )
                    continue

                for k in new_keys:
                    if len(k.split(".")) == 3 and action.destination.type!="test_result":
                        Log.error("two dots have not been needed yet, this is a consitency check")

                delete_me = old_keys - new_keys
                if delete_me:
                    if action.destination.bucket == "ekyle-test-result":
                        for k in delete_me:
                            action._destination.delete_key(k)
                    else:
                        Log.note("delete keys?\n{{list}}",  list= sorted(delete_me))
                        # for k in delete_me:
                # WE DO NOT PUT KEYS ON WORK QUEUE IF ALREADY NOTIFYING SOME OTHER
                # AND NOT GOING TO AN S3 BUCKET
                if not action._notify and isinstance(action._destination, (aws.s3.Bucket, S3Bucket)):
                    for k in old_keys | new_keys:
                        self.work_queue.add(Dict(
                            bucket=action.destination.bucket,
                            key=k
                        ))
            except Exception, e:
                if "Key {{key}} does not exist" in e:
                    err = Log.warning
                elif "multiple keys in {{bucket}}" in e:
                    err = Log.warning
                    if source_block.bucket=="ekyle-test-result":
                        for k in action._source.list(prefix=key_prefix(source_key)):
                            action._source.delete_key(strip_extension(k.key))
                elif "expecting keys to have dense order" in e:
                    err = Log.warning
                    if source_block.bucket=="ekyle-test-result":
                        # WE KNOW OF THIS ETL MISTAKE, REPROCESS
                        self.work_queue.add({
                            "key": unicode(key_prefix(source_key)),
                            "bucket": "ekyle-pulse-logger"
                        })
                elif "Expecting a pure key" in e:
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
        with self.work_queue:
            while not please_stop:
                if self.settings.wait_forever:
                    todo = None
                    while not please_stop and not todo:
                        if isinstance(self.work_queue, aws.Queue):
                            todo = self.work_queue.pop(wait=EXTRA_WAIT_TIME)
                        else:
                            todo = self.work_queue.pop()
                else:
                    if isinstance(self.work_queue, aws.Queue):
                        todo = self.work_queue.pop()
                    else:
                        todo = self.work_queue.pop(till=Date.now())
                    if todo == None:
                        please_stop.go()
                        return

                if isinstance(todo, unicode):
                    Log.warning("Work queue had {{data|json}}, which is not valid", data=todo)
                    continue

                try:
                    is_ok = self._dispatch_work(todo)
                    if is_ok:
                        self.work_queue.commit()
                    else:
                        self.work_queue.rollback()
                except Exception, e:
                    self.work_queue.rollback()
                    # WE CERTAINLY EXPECT TO GET HERE IF SHUTDOWN IS DETECTED, SHOW WARNING IF NOT THE CASE
                    if "Shutdown detected." not in e:
                        Log.warning("could not processs {{key}}.  Returned back to work queue.", key=todo.key, cause=e)

sinks_locker = Lock()
sinks = []  # LIST OF (settings, sink) PAIRS

''
def get_container(settings):
    if isinstance(settings, (MultiDayIndex, aws.s3.Bucket)):
        return settings

    if settings == None:
        return DummySink()
    elif settings.type == "redshift":
        for e in sinks:
            try:
                fuzzytestcase.assertAlmostEqual(e[0], settings)
                return e[1]
            except Exception, _:
                pass
        sink = Json2Redshift(settings=settings)
        # sink = Threaded(sink)
        sinks.append((settings, sink))
        return sink
    elif coalesce(settings.aws_access_key_id, settings.aws_access_key_id, settings.region):
        # ASSUME BUCKET NAME
        with sinks_locker:
            for e in sinks:
                try:
                    fuzzytestcase.assertAlmostEqual(e[0], settings)
                    return e[1]
                except Exception, _:
                    pass
            output =  S3Bucket(settings)
            sinks.append((settings, output))
            return output
    else:
        with sinks_locker:
            for e in sinks:
                try:
                    fuzzytestcase.assertAlmostEqual(e[0], settings)
                    return e[1]
                except Exception:
                    pass

            es = elasticsearch.Cluster(settings=settings).get_or_create_index(settings=settings)
            output = es.threaded_queue(max_size=2000, batch_size=1000)
            setattr(output, "keys", lambda prefix: set())

            sinks.append((settings, output))
            return output


def main():

    try:
        settings = startup.read_settings(defs=[
            {
                "name": ["--id"],
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

        hg = HgMozillaOrg(use_cache=True, settings=settings.hg)
        resources = Dict(hg=dictwrap(hg))
        stopper = Signal()
        for i in range(coalesce(settings.param.threads, 1)):
            ETL(
                name="ETL Loop " + unicode(i),
                work_queue=settings.work_queue,
                resources=resources,
                workers=settings.workers,
                settings=settings.param,
                please_stop=stopper
            )

        Thread.wait_for_shutdown_signal(stopper, allow_exit=True)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


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
                    queue.add(Dict(
                        bucket=w.source.bucket,
                        key=k
                    ))
        except Exception, e:
            if "Key {{key}} does not exist" in e:
                already_in_queue.add(id(source))
                queue.add(Dict(
                    bucket=w.source.bucket,
                    key=settings.args.id
                ))
            Log.warning("Problem", cause=e)

    resources = Dict(hg=HgMozillaOrg(settings=settings.hg))

    stopper = Signal()
    ETL(
        name="ETL Loop Test",
        work_queue=queue,
        workers=settings.workers,
        settings=settings.param,
        resources=resources,
        please_stop=stopper
    )

    aws.capture_termination_signal(stopper)
    Thread.wait_for_shutdown_signal(stopper, allow_exit=True)


def parse_id_argument(id):
    if id.find("..") >= 0:
        #range of ids
        min_, max_ = map(int, map(strings.trim, id.split("..")))
        return map(unicode, range(min_, max_ + 1))
    else:
        return [id]


if __name__ == "__main__":
    main()


