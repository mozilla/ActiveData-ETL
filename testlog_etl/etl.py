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
from pyLibrary.collections import MIN
from pyLibrary.env import elasticsearch
from pyLibrary.meta import get_function_by_name
from testlog_etl import key2etl, etl2path
from testlog_etl.dummy_sink import DummySink

from pyLibrary import aws
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log, Except
from pyLibrary.dot import nvl, listwrap
from pyLibrary.thread.threads import Thread, Signal


NOTHING_DONE ="Could not process records from {{bucket}}"


class ConcatSources(object):
    """
    MAKE MANY SOURCES LOOK LIKE ONE
    """

    def __init__(self, sources):
        self.source = sources

    def read(self):
        return "\n".join(s.read() for s in self.sources)


class ETL(Thread):
    def __init__(self, settings, please_stop):
        # FIND THE WORKERS METHODS
        for w in settings.workers:
            w.transformer = get_function_by_name(w.transformer)

        self.settings = settings
        self.work_queue = aws.Queue(self.settings.work_queue)
        Thread.__init__(self, "Main ETL Loop", self.loop, please_stop=please_stop)
        self.start()


    def _pipe(self, source_block):
        """
        source_block POINTS TO THE bucket AND key TO PROCESS
        :return: False IF THERE IS NOTHING LEFT TO DO
        """
        source_keys = listwrap(nvl(source_block.key, source_block.keys))

        work_actions = [w for w in self.settings.workers if w.source.bucket == source_block.bucket]
        if not work_actions:
            Log.note(NOTHING_DONE, {"bucket": source_block.bucket})
            return

        for action in work_actions:
            if len(source_keys) > 1:
                multi_source = get_container(action.source)
                source = ConcatSources([multi_source.get_key(k) for k in source_keys])
                source_key = MIN(source_keys[0])
            else:
                source = get_container(action.source).get_key(source_keys[0])
                source_key = source_keys[0]

            Log.note("Execute {{action}} on bucket={{source}} key={{key}}", {
                "action": action.name,
                "source": source_block.bucket,
                "key": source_key
            })
            try:
                dest_bucket = get_container(action.destination)
                # INCOMPLETE
                old_keys = dest_bucket.keys(prefix=source_block.key)
                new_keys = set(action.transformer(source_key, source, dest_bucket))

                if not new_keys and old_keys:
                    Log.error("Expecting some new keys after etl, especially if there were some old ones")

                for k in old_keys - new_keys:
                    dest_bucket.delete_key(k)

                if isinstance(dest_bucket, aws.s3.Bucket):
                    for k in old_keys | new_keys:
                        self.work_queue.add({
                            "bucket": action.destination,
                            "key": k
                        })
                self.work_queue.commit()
            except Exception, e:
                Log.error("Problem transforming {{action}} on bucket={{source}} key={{key}} to destination={{destination}}", {
                    "action": action.name,
                    "source": source_block.bucket,
                    "key": source_key,
                    "destination": action.destination.name
                }, e)

    def loop(self, please_stop):
        with self.work_queue:
            while not please_stop:
                todo = self.work_queue.pop()
                if todo == None:
                    please_stop.go()
                    return

                try:
                    self._pipe(todo)
                    self.work_queue.commit()
                except Exception, e:
                    self.work_queue.rollback()
                    if isinstance(e, Except) and e.contains(NOTHING_DONE):
                        continue
                    Log.warning("could not processs {{key}}", {"key": todo.key}, e)


def get_container(settings):
    if settings == None:
        return DummySink()

    elif nvl(settings.access_key_id, settings.aws_access_key_id):
        # ASSUME BUCKET NAME
        return aws.s3.Bucket(settings)
    else:
        output = elasticsearch.Index(settings)
        # ADD keys() SO ETL LOP CAN FIND WHAT'S GETTING REPLACED
        object.__setattr__(output, "keys", es_keys)

def es_keys(self, prefix=None):
    path = etl2path(key2etl(prefix))

    result = self.search({
        "fields":["_id"],
        "filtered":{
            "query":{"match_all":{}},
            "filter":{"and":[{"term":{"etl"+(".source"*i)+".id": v}} for i, v in enumerate(path)]}
        }
    })

    return result.hits.hits.fields._id




def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        stopper = Signal()
        threads = [None] * nvl(settings.param.threads, 1)

        for i, _ in enumerate(threads):
            threads[i] = ETL(settings, stopper)

        Thread.wait_for_shutdown_signal(stopper)

        for thread in threads:
            thread.stop()
            thread.join()
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


