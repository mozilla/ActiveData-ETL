# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division

# FOR WINDOWS INSTALL OF psycopg2
# http://stickpeople.com/projects/python/win-psycopg/2.6.0/psycopg2-2.6.0.win32-py2.7-pg9.4.1-release.exe
import psycopg2
from psycopg2.extensions import adapt
from pyLibrary import convert

from pyLibrary.debugs.logs import Log
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.sql import SQL
from pyLibrary.strings import expand_template
from pyLibrary.thread.threads import Lock


class Redshift(object):


    @use_settings
    def __init__(self, host, user, password, database=None, port=5439, settings=None):
        self.settings=settings
        self.locker = Lock()
        self.connection = None

    def _connect(self):
        self.connection=psycopg2.connect(
            database=self.settings.database,
            user=self.settings.user,
            password=self.settings.password,
            host=self.settings.host,
            port=self.settings.port
        )

    def query(self, sql, param=None):
        if param:
            sql = expand_template(sql, self.quote_param(param))
        with self.connection.cursor() as curs:
            curs.execute(sql)
            output = curs.fetchall()
        self.connection.commit()
        return output

    def execute(
        self,
        command,
        param=None,
        retry=True     # IF command FAILS, JUST THROW ERROR
    ):
        if param:
            command = expand_template(command, self.quote_param(param))

        done = False
        while not done:
            try:
                with self.locker:
                    if not self.connection:
                        self._connect()

                with self.connection.cursor() as curs:
                    curs.execute(command)
                self.connection.commit()
                done = True
            except Exception, e:
                self.connection.rollback()
                # TODO: FIGURE OUT WHY rollback() DOES NOT HELP
                self.connection.close()
                self._connect()
                if not retry:
                    Log.error("Problem with command:\n{{command|indent}}", {"command": command}, e)

    def insert(self, table_name, record):
        keys = record.keys()

        try:
            command = "INSERT INTO " + self.quote_column(table_name) + "(" + \
                      ",".join([self.quote_column(k) for k in keys]) + \
                      ") VALUES (" + \
                      ",".join([self.quote_value(record[k]) for k in keys]) + \
                      ")"

            self.execute(command)
        except Exception, e:
            Log.error("problem with record: {{record}}", {"record": record}, e)


    def insert_list(self, table_name, records):
        if not records:
            return

        columns = set()
        for r in records:
            columns |= set(r.keys())
        columns = qb.sort(columns)

        try:
            self.execute(
                "DELETE FROM " + self.quote_column(table_name) + " WHERE _id IN {{ids}}",
                {"ids": self.quote_column([r["_id"] for r in records])}
            )

            command = \
                "INSERT INTO " + self.quote_column(table_name) + "(" + \
                ",".join([self.quote_column(k) for k in columns]) + \
                ") VALUES " + ",\n".join([
                    "(" + ",".join([self.quote_value(r.get(k, None)) for k in columns]) + ")"
                    for r in records
                ])
            self.execute(command)
        except Exception, e:
            Log.error("problem with insert", e)



    def quote_param(self, param):
        output={}
        for k, v in param.items():
            if isinstance(v, SQL):
                output[k]=v.sql
            else:
                output[k]=self.quote_value(v)
        return output

    def quote_column(self, name):
        if isinstance(name, list):
            return SQL("(" + (", ".join(self.quote_value(v) for v in name)) + ")")
        return SQL('"' + name.replace('"', '""') + '"')

    def quote_value(self, value):
        if value ==None:
            return SQL("NULL")
        if isinstance(value, list):
            json = convert.value2json(value)
            return self.quote_value(json)

        if isinstance(value, basestring) and len(value) > 256:
            value = value[:256]
        return SQL(adapt(value))

    def es_type2pg_type(self, es_type):
        return PG_TYPES.get(es_type, "character varying")


PG_TYPES = {
    "boolean": "boolean",
    "double": "double precision",
    "float": "double precision",
    "string": "character varying",
    "long": "bigint"
}
