# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Trung Do (chin.bimbo@gmail.com)
#
from __future__ import division
from __future__ import unicode_literals

from pyDots import Null, listwrap
from pyLibrary import aws
from MoLogs import Log, Except
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from pyLibrary.queries.expressions import jx_expression
from pyLibrary.thread.signal import Signal
from pyLibrary.thread.threads import Thread
from pyLibrary.times.dates import Date

DEBUG = True

class S3Cache(object):

    @use_settings
    def __init__(self, db, bucket, key_format, settings):
        self.bucket = aws.s3.Bucket(settings)
        self.db = db
        details = self.db.query("PRAGMA table_info(files)")
        if not details.data:
            self.db.execute(
                "CREATE TABLE files ("
                "   bucket TEXT,"
                "   key TEXT,"
                "   name TEXT,"
                "   last_modified REAL,"
                "   size INTEGER,"
                "   annotate TEXT, "
                "   CONSTRAINT pk PRIMARY KEY (bucket, name)"
                ")"
            )
        self.settings = settings
        self.up_to_date = Signal()
        if key_format.startswith("t."):
            suffix = db.quote_value(key_format[3])
            selector = "cast(substr(name, 4, instr(substr(name, 4), " + suffix + ") - 1) as decimal)"
            prefixes = [
                {"prefix": "tc", "selector": selector},
                {"prefix": "bb", "selector": selector}
            ]
        else:
            suffix = db.quote_value(key_format[1])
            selector = "cast(substr(name, 1, instr(name, " + suffix + ") - 1) as decimal)"
            prefixes = [
                {"prefix": "", "selector": selector}
            ]

        threads = [self._top_up(**p) for p in prefixes]
        for t in threads:
            t.join()
        self.up_to_date.go()

    def _top_up(self, prefix, selector):
        def update(prefix, bucket, please_stop):
            if prefix:
                result = self.db.query(
                    "SELECT max(" + selector + ") as " + self.db.quote_column("max") +
                    "FROM files " +
                    "WHERE bucket=" + self.db.quote_value(bucket.name) +
                    " AND substr(name, 1, " + unicode(len(prefix)) + ")=" + self.db.quote_value(prefix)
                )
                maximum = result.data[0][0]
                for mp in listwrap(self.settings.min_primary):
                    if mp.startswith(prefix):
                        mini = int(mp.split(".")[1].split(":")[0])
                        if not (mini <= maximum):
                            maximum = mini

                if maximum:
                    biggest = prefix + "." + unicode(maximum)
                else:
                    biggest = prefix + "."
            else:
                result = self.db.query(
                    "SELECT max(" + selector + ") as " + self.db.quote_column("max") +
                    "FROM files "
                    "WHERE bucket=" + self.db.quote_value(bucket.name)
                )
                maximum = result.data[0][0]
                if maximum:
                    biggest = unicode(maximum)
                else:
                    biggest = None
            bad_count = 0
            for g, metas in jx.groupby(bucket.bucket.list(prefix=prefix, marker=biggest), size=100):
                if please_stop:
                    Log.error("Request to stop encountered")
                if bad_count > 100:
                    # Log.note("Bad count is {{count}}", count=bad_count)
                    Log.note("Exit because 1000 records show nothing older")
                    return
                data = []
                delete_me = []
                for meta in metas:
                    primary = int(meta.key[len(prefix):].lstrip(".").split(".")[0].split(":")[0])
                    if bucket.name == "active-data-jobs" and primary > 2000:
                        delete_me.append(meta.key)
                        continue

                    if primary < maximum:
                        continue

                    data.append((
                        bucket.name,
                        meta.key.split(".json")[0],
                        meta.key,
                        Date(meta.last_modified).unix,
                        meta.size
                    ))

                if delete_me:
                    Log.note("delete keys {{key}}", key=delete_me)
                    bucket.bucket.delete_keys(delete_me)
                    bad_count = 0

                if data:
                    bad_count = 0
                    if DEBUG:
                        Log.note("add {{num}} keys to cache for prefix {{prefix|quote}} ({{biggest}})", num=len(data), prefix=prefix, biggest=sorted(d[1] for d in data)[-1])

                    self.upsert_to_db(data)
                else:
                    bad_count += 1
            Log.note("Cache for {{bucket}} (prefix={{prefix|quote}}) is up to date", bucket=bucket.name, prefix=prefix)

        return Thread.run("top up "+self.bucket.name, update, prefix, self.bucket)

    def upsert_to_db(self, data):
        """
        INSERT DATA INTO DATABASE, IGNORE CONSTRAINT ERRORS
        :param data:
        :return:
        """
        try:
            self.db.query(
                "INSERT INTO files (bucket, key, name, last_modified, size) " +
                "\nUNION ALL\n".join(
                    "SELECT " + ",".join(self.db.quote_value(v) for v in d)
                    for d in data
                )
            )
        except Exception, e:
            if "UNIQUE constraint failed" in e:
                if len(data) == 1:
                    return  # TODO: P
                else:
                    split = int(round(len(data)/2, 0))
                    self.upsert_to_db(data[:split])
                    self.upsert_to_db(data[split:])
            else:
                Log.warning("Do not know what to do", cause=e)

