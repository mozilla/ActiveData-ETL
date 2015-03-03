# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap
from pyLibrary.meta import use_settings
from pyLibrary.queries.qb_usingES_util import INDEX_CACHE, parse_columns
from pyLibrary.sql import SQL
from pyLibrary.sql.redshift import Redshift
from pyLibrary.times.timer import Timer
from testlog_etl.reset import Version


class Json2Redshift(object):

    @use_settings
    def __init__(self, host, user, password, database=None, port=5439, settings=None):
        self.settings = settings
        self.pg = Redshift(settings)
        INDEX_CACHE[settings.table] = wrap({"name":settings.table})  # HACK TO GET parse_columns TO WORK
        columns = parse_columns(settings.table, settings.mapping.test_results.properties)
        nested = [c.name for c in columns if c.type == "nested"]
        self.columns = [c for c in columns if c.type not in ["object"] and not any(c.name.startswith(n+".") for n in nested)]

        try:
            self.pg.execute("""
                CREATE TABLE {{table_name}} (
                    "_id" character varying UNIQUE,
                    {{columns}}
                )""", {
                "table_name": self.pg.quote_column(settings.table),
                "columns": SQL(",\n".join(self.pg.quote_column(c.name) + " " + self.pg.es_type2pg_type(c.type) for c in self.columns))
            }, retry=False)
        except Exception, e:
            if "already exists" in e:
                Log.alert("Table {{table}} exists in Redshift", {"table": settings.table})
            else:
                Log.error("Could not make table", e)

    def keys(self, prefix):
        if ":" in prefix:
            pre_prefix = prefix.split(":")[0]

        candidates = self.pg.query("SELECT _id FROM {{table}} WHERE _id LIKE {{prefix}} || '%'", {
            "table": self.pg.quote_column(self.settings.table),
            "prefix": pre_prefix
        })

        output = set()
        source_key = Version(prefix)
        for k in candidates:
            if Version(k[0]) in source_key:
                output.add(k[0])

        return set(output)

    def add(self, value):
        self.extend([value])

    def extend(self, values):
        records = []
        for v in wrap(values):
            row = {"_id": v.id}
            for k, vv in v.value.leaves():
                row[k] = vv
            records.append(row)
        with Timer("Push {{num}} records to Redshift", {"num": len(records)}):
            self.pg.insert_list(self.settings.table, records)

