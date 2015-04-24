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

from pyLibrary.aws import s3
from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math
from pyLibrary.times.timer import Timer
from testlog_etl.sinks.multi_day_index import MultiDayIndex
from testlog_etl.sinks.s3_bucket import key_prefix



# COPY FROM S3 BUCKET TO ELASTICSEARCH

def copy2es(settings, work_queue, please_stop):
    # EVERYTHING FROM ELASTICSEARCH
    print "started"
    es = MultiDayIndex(settings.elasticsearch, queue_size=100000)
    bucket = s3.Bucket(settings.source)

    for block in iter(work_queue.get, "STOP"):
        if please_stop.get(False):
            return

        keys = [k.key for k in bucket.list(prefix=unicode(block) + ":")]

        extend_time = Timer("insert", silent=True)
        with extend_time:
            num_keys = es.copy(keys, bucket)

        Log.note("Added {{num}} keys from {{key}} block in {{duration|round(places=2)}} seconds ({{rate|round(places=3)}} keys/second)", {
            "num": num_keys,
            "key": key_prefix(keys[0]),
            "duration": extend_time.seconds,
            "rate": num_keys / Math.max(extend_time.seconds, 1)
        })



