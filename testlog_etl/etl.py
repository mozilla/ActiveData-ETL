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
from copy import deepcopy
import sys
import gc
from pyLibrary.collections import MIN
from pyLibrary.env import elasticsearch
from pyLibrary.meta import get_function_by_name, use_settings
from pyLibrary.times.durations import Duration
from testlog_etl import key2etl, etl2path
from testlog_etl.dummy_sink import DummySink

from pyLibrary import aws
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log, Except
from pyLibrary import dot
from pyLibrary.dot import nvl, listwrap
from pyLibrary.thread.threads import Thread, Signal


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
        please_stop,
        wait_forever=False,
        settings=None
    ):
        # FIND THE WORKERS METHODS
        settings.workers = deepcopy(workers)
        for w in settings.workers:
            w.transformer = dot.get_attr(sys.modules, w.transformer)
            w._source = get_container(w.source)
            w._destination = get_container(w.destination)

        self.settings = settings
        self.work_queue = aws.Queue(work_queue)
        Thread.__init__(self, name, self.loop, please_stop=please_stop)
        self.start()


    def _dispatch_work(self, source_block):
        """
        source_block POINTS TO THE bucket AND key TO PROCESS
        :return: False IF THERE IS NOTHING LEFT TO DO
        """
        source_keys = listwrap(nvl(source_block.key, source_block.keys))

        if not isinstance(source_block.bucket, basestring):  # FIX MISTAKE
            source_block.bucket = source_block.bucket.bucket
        bucket = source_block.bucket
        work_actions = [w for w in self.settings.workers if w.source.bucket == bucket]

        if not work_actions:
            Log.note("No worker defined for records from {{bucket}}, skipping.\n{{message|indent}}", {
                "bucket": source_block.bucket,
                "message": source_block
            })
            return not self.settings.keep_unknown_on_queue

        for action in work_actions:
            if len(source_keys) > 1:
                multi_source = action._source
                source = ConcatSources([multi_source.get_key(k) for k in source_keys])
                source_key = MIN(source_keys[0])
            else:
                source = action._source.get_key(source_keys[0])
                source_key = source_keys[0]

            Log.note("Execute {{action}} on bucket={{source}} key={{key}}", {
                "action": action.name,
                "source": source_block.bucket,
                "key": source_key
            })
            try:
                new_keys = set(action.transformer(source_key, source, action._destination))

                old_keys = action._destination.keys(prefix=source_block.key)
                if not new_keys and old_keys:
                    Log.alert("Expecting some new keys after etl of {{source_key}}, especially since there were old ones\n{{old_keys}}", {
                        "old_keys": old_keys,
                        "source_key": source_key
                    })
                    continue
                elif not new_keys:
                    Log.alert("Expecting some new keys after processing {{source_key}}", {
                        "old_keys": old_keys,
                        "source_key": source_key
                    })
                    continue

                delete_me = old_keys - new_keys
                if delete_me:
                    Log.note("delete keys?\n{{list}}", {"list": sorted(delete_me)})
                    for k in delete_me:
                        pass
                        # dest_bucket.delete_key(k)

                if isinstance(action._destination, aws.s3.Bucket):
                    for k in old_keys | new_keys:
                        self.work_queue.add({
                            "bucket": action.destination.bucket,
                            "key": k
                        })
            except Exception, e:
                Log.error("Problem transforming {{action}} on bucket={{source}} key={{key}} to destination={{destination}}", {
                    "action": action.name,
                    "source": source_block.bucket,
                    "key": source_key,
                    "destination": nvl(action.destination.name, action.destination.index)
                }, e)
        return True

    def loop(self, please_stop):
        with self.work_queue:
            while not please_stop:
                if self.settings.wait_forever:
                    todo = None
                    while not please_stop and not todo:
                        todo = self.work_queue.pop(wait=EXTRA_WAIT_TIME)
                else:
                    todo = self.work_queue.pop()
                    if todo == None:
                        please_stop.go()
                        return

                try:
                    is_ok = self._dispatch_work(todo)
                    if is_ok:
                        self.work_queue.commit()
                    else:
                        self.work_queue.rollback()
                except Exception, e:
                    self.work_queue.rollback()
                    Log.warning("could not processs {{key}}", {"key": todo.key}, e)


def get_container(settings):
    if isinstance(settings, (Index_w_Keys, aws.s3.Bucket)):
        return settings

    if settings == None:
        return DummySink()

    elif nvl(settings.aws_access_key_id, settings.aws_access_key_id):
        # ASSUME BUCKET NAME
        return aws.s3.Bucket(settings)
    else:
        elasticsearch.Cluster(settings).get_or_create_index(settings)
        return Index_w_Keys(settings)


class Index_w_Keys(elasticsearch.Index):
    def __init__(self, settings):
        elasticsearch.Index.__init__(self, settings)

    # ADD keys() SO ETL LOOP CAN FIND WHAT'S GETTING REPLACED
    def keys(self, prefix=None):
        path = etl2path(key2etl(prefix))

        result = self.search({
            "fields": ["_id"],
            "query": {
                "filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [{"term": {"etl" + (".source" * i) + ".id": v}} for i, v in enumerate(path)]}
                }
            }
        })

        return set(result.hits.hits.fields._id)


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        constants.set(settings.constants)

        stopper = Signal()
        threads = [None] * nvl(settings.param.threads, 1)

        for i, _ in enumerate(list(threads)):
            threads[i] = ETL(
                name="ETL Loop " + unicode(i),
                work_queue=settings.work_queue,
                workers=settings.workers,
                settings=settings.param,
                please_stop=stopper
            )

        wait_for_exit(stopper)
        Thread.wait_for_shutdown_signal(stopper)

        for thread in threads:
            thread.stop()
            thread.join()
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()




def readloop(please_stop):
    while not please_stop:
        command = sys.stdin.readline()
        if command.strip() == "exit":
            break
    please_stop.go()

def wait_for_exit(please_stop):
    Thread('waiting for "exit"', readloop, please_stop=please_stop).start()






if __name__ == "__main__":
    main()


