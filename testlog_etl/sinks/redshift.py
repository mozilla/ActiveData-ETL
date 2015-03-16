# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary import convert
from pyLibrary.aws import s3

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, split_field
from pyLibrary.meta import use_settings
from pyLibrary.queries.qb_usingES_util import INDEX_CACHE, parse_columns
from pyLibrary.sql import SQL
from pyLibrary.sql.redshift import Redshift
from pyLibrary.times.timer import Timer
from testlog_etl.reset import Version
from testlog_etl.sinks.s3_bucket import key_prefix


class Json2Redshift(object):

    @use_settings
    def __init__(
        self,
        host,
        user,
        password,
        table,
        meta,       # REDSHIFT COPY COMMAND REQUIRES A BUCKET TO HOLD PARAMETERS
        database=None,
        port=5439,
        settings=None
    ):
        self.settings = settings
        self.db = Redshift(settings)
        INDEX_CACHE[settings.table] = wrap({"name":settings.table})  # HACK TO GET parse_columns TO WORK
        columns = parse_columns(settings.table, settings.mapping.test_result.properties)
        nested = [c.name for c in columns if c.type == "nested"]
        self.columns = wrap([c for c in columns if c.type not in ["object"] and not any(c.name.startswith(n+".") for n in nested)])

        try:
            self.db.execute("""
                CREATE TABLE {{table_name}} (
                    "_id" character varying UNIQUE,
                    {{columns}}
                )""", {
                "table_name": self.db.quote_column(settings.table),
                "columns": SQL(",\n".join(self.db.quote_column(c.name) + " " + self.db.es_type2pg_type(c.type) for c in self.columns))
            }, retry=False)
        except Exception, e:
            if "already exists" in e:
                Log.alert("Table {{table}} exists in Redshift", {"table": settings.table})
            else:
                Log.error("Could not make table", e)


        # MAKE jsonpaths FOR COPY COMMAND
        jsonpaths = {"jsonpaths": [
            "$" + "".join("[" + convert.string2quote(p) + "]" for p in split_field(c.name)) for c in self.columns
        ]}
        content = convert.value2json(jsonpaths)
        content = content.replace("\\\"", "'")
        # PUSH TO S3
        s3.Bucket(meta).write(meta.jsonspath, content)

    def keys(self, prefix):
        candidates = self.db.query("SELECT _id FROM {{table}} WHERE _id LIKE {{prefix}} || '%'", {
            "table": self.db.quote_column(self.settings.table),
            "prefix": key_prefix(prefix)
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
            self.db.insert_list(self.settings.table, records)


    def copy(self, key, source):
        self.db.execute(
            """
                COPY {{table_name}} ({{columns}})
                FROM {{s3_source}}
                CREDENTIALS {{credentials}}
                JSON {{jsonspath}}
                TRUNCATECOLUMNS
                GZIP
            """,
            {
                "s3_source": "s3://" + self.settings.source.bucket + "/" + key,
                "table_name": self.db.quote_column(self.settings.table),
                "columns": SQL(",".join(map(self.db.quote_column, self.columns.name))),
                "credentials": "aws_access_key_id=" + self.settings.meta.aws_access_key_id + ";aws_secret_access_key=" + self.settings.meta.aws_secret_access_key,
                "jsonspath": "s3://" + self.settings.meta.bucket + "/" + self.settings.meta.jsonspath + ".json"
            },
            retry=False
        )
