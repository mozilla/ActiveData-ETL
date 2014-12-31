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

from testlog_etl.transforms.pulse_block_to_unittest_logs import process_pulse_block
from testlog_etl.transforms.pulse_block_to_talos_logs import process_talos

from pyLibrary import aws
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.structs import wrap, nvl

from pyLibrary.structs.wraps import listwrap
from pyLibrary.thread.threads import Thread
from testlog_etl import key2etl

workers = wrap([
    {
        "name": "pulse2unittest",
        "source": "all-pulse-testing",
        "destination": "all-unittest-testing",
        "transformer": process_pulse_block,
        "type": "join"
    },
    {
        "name": "pulse2talos",
        "source": "all-pulse-testing",
        "destination": "etl-talos-testing",
        "transformer": process_talos,
        "type": "join"
    }
])


class ConcatSources(object):
    """
    MAKE MANY SOURCES LOOK LIKE ONE
    """

    def __init__(self, sources):
        self.source = sources

    def read(self):
        return "\n".join(s.read() for s in self.sources)


class ETL(Thread):
    def __init__(self, settings):
        self.settings = settings
        self.work_queue = aws.Queue(self.settings.work_queue)
        self.connection = aws.s3.Connection(self.settings.aws)
        Thread.__init__(self, "Main ETL Loop", self.loop)
        self.start()


    def pipe(self, source_block):
        """
        :return: False IF THERE IS NOTHING LEFT TO DO
        """
        source_keys = listwrap(nvl(source_block.key, source_block.keys))

        work_actions = [w for w in workers if w.source == source_block.bucket]
        if not work_actions:
            Log.error("Could not process records from {{bucket}}", {"bucket": source_block.bucket})

        if len(source_keys) > 1:
            source = ConcatSources([self.connection.get_bucket(source_block.bucket).get_key(k) for k in source_keys])
            source_key = MIN(source_keys[0])
        else:
            source = self.connection.get_bucket(source_block.bucket).get_key(source_keys[0])
            source_key = source_keys[0]

        for action in work_actions:
            Log.note("Execute {{action}} on bucket={{source}} key={{key}}", {
                "action": action.name,
                "source": source_block.bucket,
                "key": source_key
            })
            try:
                dest_bucket = self.connection.get_bucket(action.destination)
                # INCOMPLETE
                old_keys = dest_bucket.keys(prefix=source_block.key)
                new_keys = set(action.transformer(source_key, source, dest_bucket))

                for k in old_keys - new_keys:
                    dest_bucket.delete_key(k)

                for k in old_keys | new_keys:
                    self.work_queue.add({
                        "bucket": action.destination,
                        "key": k
                    })
                self.work_queue.commit()
            except Exception, e:
                Log.error("Problem transforming", e)


    def loop(self, please_stop):
        with self.work_queue:
            with self.connection:
                while not please_stop:
                    todo = self.work_queue.pop()
                    if todo == None:
                        return

                    try:
                        self.pipe(todo)
                        self.work_queue.commit()
                    except Exception, e:
                        Log.warning("could not processs {{key}}", {"key": todo.key}, e)


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        thread = ETL(settings)
        Thread.wait_for_shutdown_signal(thread.please_stop)
        thread.stop()
        thread.join()
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


