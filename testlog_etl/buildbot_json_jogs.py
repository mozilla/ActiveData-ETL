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
import zlib

from pyLibrary import convert, strings
from pyLibrary.aws import s3, Queue
from pyLibrary.convert import string2datetime
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict
from pyLibrary.env import http
from pyLibrary.jsons import stream
from pyLibrary.maths import Math
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY
from testlog_etl.imports.buildbot import BuildbotTranslator


ACTIVE_DATA = "http://activedata.allizom.org/query"
DEBUG = True


def parse_to_s3(settings):
    paths = get_all_logs(settings.source.url)
    for path in paths:
        try:
            parse_day(settings, path)
        except Exception, e:
            Log.warning("problem with {{path}}", path=path, cause=e)


def random(settings):
    paths = get_all_logs(settings.source.url)
    while True:
        path = Random.sample(paths[1::], 1)[0]
        try:
            parse_day(settings, path)
        except Exception, e:
            Log.warning("problem with {{path}}", path=path, cause=e)


def parse_day(settings, p, force=False):
    bb_translator = BuildbotTranslator()
    destination = s3.Bucket(settings.destination)
    notify = Queue(settings=settings.notify)

    # DATE TO DAYS-SINCE-2000
    day = Date(string2datetime(p[7:17], format="%Y-%m-%d"))
    day_num = (day - Date("1 JAN 2015")) / DAY
    day_url = settings.source.url + p

    if day_num < 0:
        return
    if day > Date.today():
        return

    if force:
        try:
            destination.delete_key(unicode(day_num) + ".0")
        except Exception, e:
            pass
    else:
        # CHECK TO SEE IF THIS DAY WAS DONE
        if destination.get_meta(unicode(day_num) + ".0"):
            return

    Log.note("Processing {{url}}", url=day_url)
    day_etl = Dict(
        id=day_num,
        url=day_url,
        timestamp=Date.now()
    )
    tasks = get_all_tasks(day_url)
    first = None
    for group_number, ts in qb.groupby(tasks, size=100):
        parsed = []
        for row_number, t in enumerate(ts):
            row_etl = Dict(
                timestamp=Date.now(),
                id=row_number,
                source=day_etl
            )
            try:
                d = bb_translator.parse(t['builds'])
                d.etl = row_etl
                parsed.append(convert.value2json(t['builds']))
            except Exception, e:
                d = {"etl": row_etl}
                parsed.append(convert.value2json(d))
                Log.warning("problem in {{path}}", path=day_url, cause=e)

        if group_number == 0:
            # WRITE THE FIRST BLOCK (BLOCK 0) LAST
            first = parsed
            continue

        key = unicode(day_num) + "." + unicode(group_number)
        destination.write_lines(key=key, lines=parsed)
        notify.add({"key": key, "bucket": destination.name, "timestamp": Date.now()})

    if first == None:
        Log.error("How did this happen?")

    # WRITE FIRST BLOCK
    key = unicode(day_num) + ".0"
    destination.write_lines(key=key, lines=first)
    notify.add({"key": key, "bucket": destination.name, "timestamp": Date.now()})




def get_all_logs(url):
    # GET LIST OF LOGS
    paths = []
    response = http.get(url)
    for line in response.all_lines:
        # <tr><td valign="top"><img src="/icons/compressed.gif" alt="[   ]"></td><td><a href="builds-2015-09-20.js.gz">builds-2015-09-20.js.gz</a></td><td align="right">20-Sep-2015 19:00  </td><td align="right">6.9M</td><td>&nbsp;</td></tr>
        filename = strings.between(line, '</td><td><a href=\"', '">')
        if filename and filename.startswith("builds-2"):  # ONLY INTERESTED IN DAILY SUMMARY FILES (eg builds-2015-09-20.js.gz)
            paths.append(filename)
        paths = qb.reverse(qb.sort(paths))
    return paths


def get_all_tasks(url):
    """
    RETURN ITERATOR OF ALL `builds` IN THE BUILDBOT JSON LOG
    """
    response = http.get(url)
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    def json():
        last_bytes_count = 0  # Track the last byte count, so we do not show too many
        bytes_count = 0
        while True:
            bytes_ = response.raw.read(4096)
            if not bytes_:
                return
            data = decompressor.decompress(bytes_)
            bytes_count += len(data)
            if Math.floor(last_bytes_count, 1000000) != Math.floor(bytes_count, 1000000):
                last_bytes_count = bytes_count
                if DEBUG:
                    Log.note("bytes={{bytes}}", bytes=bytes_count)
            yield data

    return stream.parse(
        json(),
        "builds",
        expected_vars=["builds"]
    )




def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        parse_to_s3(settings)
        # random(settings)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
