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

import sys

from activedata_etl.imports.s3_cache import S3Cache
from mo_dots import Data
from pyLibrary import aws, convert
from mo_logs import startup, constants
from mo_logs import Log
from pyLibrary.env import http
from jx_python import jx
from pyLibrary.sql.sqlite import Sqlite
from mo_threads import Thread
from mo_threads import Till
from mo_times.dates import Date
from mo_times.durations import Duration
from mo_times.timer import Timer

ACTIVE_DATA = "http://activedata.allizom.org/query"
RUN_TIME = 10 * 60
MAX_SIZE = 10000
QUOTED_INVALID = Sqlite().quote_value(convert.value2json("invalid"))


def backfill_recent(cache, settings, index_queue, please_stop):
    db_filename = cache + "." + settings.source.bucket + ".sqlite"
    db = Sqlite(db_filename)
    bucket = S3Cache(db=db, kwargs=settings.source)
    prime_id = settings.rollover.field
    backfill = Data(total=0)
    too_old = (Date.now().floor(Duration(settings.rollover.interval)) - Duration(settings.rollover.max))

    def get_in_s3(prefix):
        result = db.query(
            " SELECT " +
            "    key, annotate"
            " FROM files " +
            " WHERE substr(name, 1, " + unicode(len(prefix)) + ")=" + db.quote_value(prefix) +
            " AND (annotate is NULL OR annotate <> " + QUOTED_INVALID + ")" +
            " AND last_modified > " + db.quote_value(too_old.unix)
        )
        return set(d[0] for d in result.data)

    def decimate(prefix, please_stop):
        if backfill.total > MAX_SIZE:
            return

        # HOW MANY WITH GIVEN PREFIX?
        result = db.query(
            " SELECT " +
            "    substr(name, 1, " + unicode(len(prefix) + 1) + ") as prefix," +
            "    count(1) as number, " +
            "    avg(last_modified) as `avg` " +
            " FROM files " +
            " WHERE substr(name, 1, " + unicode(len(prefix)) + ")=" + db.quote_value(prefix) +
            " AND (annotate is NULL OR annotate <> " + QUOTED_INVALID + ")" +
            " AND last_modified > " + db.quote_value(too_old.unix) +
            " GROUP BY substr(name, 1, " + unicode(len(prefix) + 1) + ")"
        )

        # TODO: PULL THE SAME COUNTS FROM ES, BUT GROUPBY ON _id IS BROKEN

        for prefix2, count, timestamp in list(reversed(sorted(result.data, key=lambda d: d[2]))):
            if count < MAX_SIZE:
                fill_holes(prefix2, please_stop)
            else:
                Log.note(
                    "Decimate prefix={{prefix|quote}}, count={{count}}, avg(timestamp)={{timestamp|datetime}}",
                    prefix=prefix2,
                    timestamp=timestamp,
                    count=count
                )
                decimate(prefix2, please_stop)

    def fill_holes(prefix, please_stop):
        result = http.post_json(ACTIVE_DATA, json={
            "from": settings.elasticsearch.index,
            "select": ["_id", {"name": "value", "value": prime_id}],
            "where": {"and": [
                {"eq": {"etl.id": 0}},
                {"prefix": {"_id": prefix}}
            ]},
            "limit": 2 * MAX_SIZE,
            "format": "list"
        })

        in_es = set(".".join(i.split(".")[:-1]) for i in result.data._id)
        in_s3 = get_in_s3(prefix)

        keys = list(reversed(sorted(in_s3 - in_es)))

        if not keys:
            return

        with Timer("adding {{num}} keys from {{bucket}} with prefix {{prefix}}", param={"num": len(keys), "bucket": settings.source.bucket, "prefix": prefix}):
            invalid = set()
            for k in keys:
                if please_stop:
                    Log.error("Asked to stop")
                try:
                    bucket.bucket._verify_key_format(k)
                except Exception:
                    invalid.add(k)
                    continue
                now = Date.now()
                index_queue.add({
                    "bucket": settings.source.bucket,
                    "key": k,
                    "timestamp": now.unix,
                    "date/time": now.format()
                })
        if invalid:
            Log.note("{{num}} invalid keys", num=len(invalid))
            for g, some in jx.groupby(invalid, size=100):
                db.execute(
                    "UPDATE files SET annotate=" + QUOTED_INVALID + " WHERE key in (" +
                    ",".join(db.quote_value(k) for k in some) +
                    ")"
                )
        backfill.total += len(keys) - len(invalid)

    timeout = Till(seconds=RUN_TIME)
    if not settings.backfill.disabled:
        decimate("", please_stop)
    (timeout | bucket.up_to_date).wait()
    Log.note("done")


def main():
    """
    RE INDEX DATA FROM S3
    IF THE ETL IS GOOD, AND YOU JUST NEED TO FILL ES, USE THIS
    """
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
        queue = aws.Queue(settings.work_queue)

        threads = [
            Thread.run("backfill " + w.name, backfill_recent, settings.cache, w, queue)
            for w in settings.workers
        ]

        for t in threads:
            t.join()
        sys.stdout.write("main done\n")
        Log.note("main done")
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
