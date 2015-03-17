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
from pyLibrary import queries

from pyLibrary.aws import s3
from pyLibrary.aws.s3 import strip_extension

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import qb
from pyLibrary.times.timer import Timer
from testlog_etl.sinks.multi_day_index import MultiDayIndex
from testlog_etl.sinks.s3_bucket import key_prefix


# COPY FROM S3 BUCKET TO REDSHIFT


def diff(settings):
    # EVERYTHING FROM REDSHIFT
    es = MultiDayIndex(settings.elasticsearch)

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

    in_rs = set(result.aggregations._match.buckets.key)

    # EVERYTHING FROM S3
    bucket = s3.Bucket(settings.source)
    prefixes = [p.name.rstrip(":") for p in bucket.list(prefix="", delimiter=":")]
    in_s3 = []
    for i, p in enumerate(prefixes):
        if i % 1000 == 0:
            Log.note("Scrubbed {{p|percent(digits=2)}}", {"p": i / len(prefixes)})
        try:
            if int(p) not in in_rs:
                in_s3.append(int(p))
        except Exception, _:
            Log.note("delete key {{key}}", {"key": p})
            bucket.delete_key(strip_extension(p))
    in_s3 = qb.reverse(qb.sort(in_s3))

    for block in in_s3:
        keys = [k.key for k in bucket.list(prefix=unicode(block) + ":")]

        extend_time = Timer("insert", silent=True)
        with extend_time:
            num_keys = es.copy(keys, bucket)

        Log.note("Added {{num}} keys from {{key}} block in {{duration|round(places=2)}} seconds ({{rate|round(places=3)}} keys/second)", {
            "num": num_keys,
            "key": key_prefix(keys[0]),
            "duration": extend_time.seconds,
            "rate": num_keys/extend_time.seconds
        })



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

        diff(settings)

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
