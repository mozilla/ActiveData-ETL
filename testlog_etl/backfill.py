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

from pyLibrary import aws
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict, literal_field
from pyLibrary.maths import Math
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date
from testlog_etl.sinks.s3_bucket import key_prefix
from testlog_etl.etl import get_container


MAX_QUEUE_SIZE = 1000
ETL_DEPTH = 4
START_KEY = 50000
MIN_KEY = 0
BLOCK_SIZE = 100

done_min = START_KEY
done_max = START_KEY
current_revision = []
counter = Dict()


def setup(source, destination, settings):
    global done_min
    global done_max

    # FIND LARGEST ID IN DESTINATION
    max_id = destination.find_largest_key()

    # USE AS STARTING POINT
    if max_id:
        max_id = Math.floor(max_id, BLOCK_SIZE)
        done_min = done_max = max_id

        # FIND CURRENT REVISION


def backfill(source, destination, work_queue, settings):
    global done_min
    global done_max

    setup(source, destination, settings)

    while True:
        # CAN WE RUN?
        wait_for_queue(work_queue)

        # FIND FUTURE IDS FIRST
        while True:
            new_keys = source.find_keys(done_max, BLOCK_SIZE)
            if not new_keys:
                break
            Log.note("Add {{num}} new keys", {"num": len(new_keys)})
            add_to_queue(work_queue, new_keys, source.settings.bucket)
            done_max += BLOCK_SIZE
            wait_for_queue(work_queue)

        # BACKFILL
        while done_min >= MIN_KEY:
            done = destination.find_keys(done_min - BLOCK_SIZE, BLOCK_SIZE, filter={"term": {"etl.revision": current_revision}})
            done = set(map(key_prefix, done))
            existing = source.find_keys(done_min - BLOCK_SIZE, BLOCK_SIZE)
            existing = set(map(key_prefix, existing))

            Log.note("verified {{block}} block", {"block": done_min})
            redo = existing - done
            done_min -= BLOCK_SIZE
            if redo:
                Log.note("Refreshing {{num}} keys", {"num": len(redo)})
                add_to_queue(work_queue, map(unicode, redo), source.settings.bucket)
                wait_for_queue(work_queue)


def wait_for_queue(work_queue):
    """
    SLEEP UNTIL WORK QUEU IS EMPTY ENOUGH FOR MORE
    """
    # return
    while True:
        if len(work_queue) < MAX_QUEUE_SIZE:
            break
        Thread.sleep(seconds=5 * 60)


def add_to_queue(work_queue, redo, bucket_name):
    now = Date.now()
    for r in redo:
        k = literal_field(r)
        counter[k] += 1
        if counter[k] > 3:
            Log.error("Problem backfilling {{key}}: Tried >=3 times, giving up", {"key": r})
            continue

        work_queue.add({
            "bucket": bucket_name,
            "key": r,
            "timestamp": now.unix,
            "date/time": now.format()
        })


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        source = get_container(settings.source)
        destination = get_container(settings.destination)

        work_queue = aws.Queue(settings.work_queue)
        backfill(source, destination, work_queue, settings)
    except Exception, e:
        Log.error("Problem with backfill", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
