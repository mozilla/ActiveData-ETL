# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary.aws import s3
from pyLibrary.aws.s3 import strip_extension

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from testlog_etl.sinks.s3_bucket import key_prefix
from testlog_etl.transforms.test_result_to_redshift import CopyToRedshift


def diff(settings):
    # EVERYTHING FROM REDSHIFT
    rs = CopyToRedshift(settings)
    in_rs = rs.db.query("""SELECT DISTINCT "etl.source.source.id" FROM test_results""")
    in_rs = set(key_prefix(r[0]) for r in in_rs)

    # EVERYTHING FROM S3
    bucket = s3.Bucket(settings.source)
    prefixes = bucket.keys(prefix="", delimiter=":")
    in_s3 = set()
    for p in prefixes:
        try:
            in_s3.add(int(p))
        except Exception, _:
            bucket.delete_key(strip_extension(p))

    # REMOVE OVERLAP
    diff = in_s3 - in_rs
    rs.db.execute("""DELETE FROM test_results WHERE "etl.source.source.id" IN {{diff}}""", {
        "diff": rs.quote_column(diff)
    })

    # PUSH DIFFERENCES
    rs.extend(diff)


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
