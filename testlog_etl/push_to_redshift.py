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
from pyLibrary.aws.s3 import strip_extension

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import qb
from pyLibrary.times.timer import Timer
from testlog_etl.sinks.s3_bucket import key_prefix
from testlog_etl.transforms.test_result_to_redshift import CopyToRedshift


def diff(settings):
    # EVERYTHING FROM REDSHIFT
    rs = CopyToRedshift(settings)

    def count():
        return rs.db.execute("SELECT COUNT(1) FROM {{table}}", {"table": rs.db.quote_column(settings.redshift.table)})[0][0]

    in_rs = rs.db.query("""SELECT DISTINCT "etl.source.source.id" FROM test_results""")
    in_rs = set(key_prefix(r[0]) for r in in_rs if r[0] != None)

    # EVERYTHING FROM S3
    bucket = s3.Bucket(settings.source)
    prefixes = [p.name.rstrip(":") for p in bucket.list(prefix="", delimiter=":")]
    in_s3 = []
    for i, p in enumerate(prefixes):
        if i % 1000 == 0:
            Log.note("Scrubbed {{p|percent(digits=2)}}", {"p": i / len(prefixes)})
        try:
            if int(p) in in_rs:
                in_s3.append(int(p))
        except Exception, _:
            Log.note("delete key {{key}}", {"key": p})
            bucket.delete_key(strip_extension(p))
    in_s3 = qb.reverse(qb.sort(in_s3))

    old_count = count()
    for g, block in qb.groupby(in_s3, size=10):
        keys = []
        for k in block:
            keys.extend(k.key for k in bucket.list(prefix=unicode(k) + ":"))

        extend_time = Timer("insert", silent=True)
        with extend_time:
            rs.extend(keys)
            new_count = count()

        Log.note("Added {{num}} keys from {{key}} block in {{duration|round(places=2)}} seconds ({{rate|round(places=3)}} keys/second)", {
            "num": new_count - old_count,
            "key": key_prefix(keys[0]),
            "duration": extend_time.seconds,
            "rate": (new_count - old_count)/extend_time.seconds
        })
        old_count = new_count


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

        if settings.args.id:
            if settings.args.id == "all":
                settings.args.id = ""

            pusher = CopyToRedshift(settings)
            pusher.add(settings.args.id)
            return

        diff(settings)

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
