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

from pyLibrary import aws
from pyLibrary.debugs.logs import Log
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
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
                "   annotate TEXT"
                ")"
            )
        self.settings = settings
        self.up_to_date = Signal()
        if key_format.startswith("t."):
            prefixes = ["tc", "bb"]
        else:
            prefixes = [""]

        threads = [self._top_up(p) for p in prefixes]
        for t in threads:
            t.join()
        self.up_to_date.go()

    def _top_up(self, prefix):
        def update(prefix, bucket, please_stop):
            result = self.db.query(
                "SELECT max(name) as \"max\""
                "FROM files "
                "WHERE bucket=" + self.db.quote_value(bucket.name) +
                " AND substr(name, 1, " + unicode(len(prefix)) + ")=" + self.db.quote_value(prefix)
            )

            for g, metas in jx.groupby(bucket.bucket.list(prefix=prefix, marker=result.data[0][0]), size=100):
                if please_stop:
                    Log.error("Request to stop encountered")
                if DEBUG:
                    Log.note("add {{num}} keys to cache for prefix {{prefix|quote}}", num=len(metas), prefix=prefix)
                self.db.execute(
                    "INSERT INTO files (bucket, key, name, last_modified, size) " +
                    "\nUNION ALL\n".join(
                        "SELECT " + ",".join(
                            self.db.quote_value(v)
                            for v in [
                                bucket.name,
                                meta.key.split(".json")[0],
                                meta.key,
                                Date(meta.last_modified).unix,
                                meta.size
                            ])
                        for meta in metas
                    )
                )
            Log.note("Cache for {{bucket}} (prefix={{prefix|quote}}) is up to date", bucket=bucket.name, prefix=prefix)

        return Thread.run("top up "+self.bucket.name, update, prefix, self.bucket)
