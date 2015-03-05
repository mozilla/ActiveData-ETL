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
from pyLibrary.dot import wrap, split_field
from pyLibrary.maths.randoms import Random
from pyLibrary.meta import use_settings
from pyLibrary.queries.qb_usingES_util import parse_columns, INDEX_CACHE
from pyLibrary.sql import SQL
from pyLibrary.sql.redshift import Redshift


class PushToRedshift(object):

    @use_settings
    def __init__(self, redshift, meta, settings=None):
        # MAKE MAPPING FILE
        self.pg = Redshift(redshift)
        self.settings = settings
        INDEX_CACHE[redshift.table] = wrap({"name": redshift.table})  # HACK TO GET parse_columns TO WORK
        columns = parse_columns(redshift.table, redshift.mapping.test_results.properties)
        nested = [c.name for c in columns if c.type == "nested"]
        self.columns = wrap([{"name": "_id", "type": "string"}] + [c for c in columns if c.type not in ["object"] and not any(c.name.startswith(n + ".") for n in nested)])

        # CONVERT TO jsonpaths
        jsonpaths = {"jsonpaths": [
            "$" + "".join("[" + convert.string2quote(p) + "]" for p in split_field(c.name)) for c in self.columns
        ]}
        content=convert.value2json(jsonpaths)
        content=content.replace("\\\"", "'")

        # PUSH MAPPING TO S3
        s3.Bucket(meta).write(meta.jsonspath, content)

        self.db = Redshift(self.settings.redshift)

    def add(self, key):
        # SEND COMMAND TO REDSHIFT TO LOAD IT

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
                "table_name": self.db.quote_column(self.settings.redshift.table),
                "columns": SQL(",".join(map(self.db.quote_column, self.columns.name))),
                "credentials": "aws_access_key_id=" + self.settings.meta.aws_access_key_id + ";aws_secret_access_key=" + self.settings.meta.aws_secret_access_key,
                "jsonspath": "s3://" + self.settings.meta.bucket + "/" + self.settings.meta.jsonspath + ".json"
            },
            retry=False
        )

    def extend(self, keys):
        keyname = "add_to_redshift_" + Random.hex(20)
        manifest = {"entries": [{"url": "s3://" + self.settings.source.bucket + "/" + k} for k in keys]}
        s3.Bucket(self.settings.meta).write(keyname, convert.value2json(manifest))

        self.db.execute("""
            COPY {{table_name}} ({{columns}})
            FROM {{s3_source}}
            CREDENTIALS {{credentials}}
            JSON {{jsonspath}}
            TRUNCATECOLUMNS
            GZIP
            """, {
            "s3_source": "s3://" + self.meta.bucket + "/" + keyname+ ".json",
            "table_name": self.db.quote_column(self.settings.redshift.table),
            "columns": SQL(",".join(self.db.quote_column(self.columns.name))),
            "credentials": "aws_access_key_id=" + self.settings.meta.aws_access_key_id + ";aws_secret_access_key=" + self.settings.meta.aws_secret_access_key,
            "jsonspath": "s3://" + self.settings.meta.bucket + "/" + self.settings.meta.jsonspath + ".json"
        }
        )
