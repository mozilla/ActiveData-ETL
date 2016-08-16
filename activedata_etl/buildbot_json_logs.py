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

from tempfile import TemporaryFile

from pyLibrary import convert, strings
from pyLibrary.aws import s3, Queue
from pyLibrary.convert import string2datetime
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.exceptions import suppress_exception, Explanation
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict
from pyLibrary.env import http
from pyLibrary.env.big_data import scompressed2ibytes
from pyLibrary.jsons import stream
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import jx
from pyLibrary.thread.threads import Thread, Lock
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY
from pyLibrary.times.timer import Timer

REFERENCE_DATE = Date("1 JAN 2015")
EARLIEST_CONSIDERATION_DATE = Date.today() - (90 * DAY)
ACTIVE_DATA = "http://activedata.allizom.org/query"
DEBUG = True


def parse_to_s3(settings):
    paths = get_all_logs(settings.source.url)
    for path in paths:
        try:
            parse_day(settings, path, settings.force)
        except Exception, e:
            day = Date(string2datetime(path[7:17], format="%Y-%m-%d"))
            day_num = int((day - REFERENCE_DATE) / DAY)

            Log.warning("Problem with #{{num}}: {{path}}", path=path, num=day_num, cause=e)


def random(settings):
    paths = get_all_logs(settings.source.url)
    while True:
        path = Random.sample(paths[1::], 1)[0]
        try:
            parse_day(settings, path, force=True)
        except Exception, e:
            Log.warning("problem with {{path}}", path=path, cause=e)


def parse_day(settings, p, force=False):
    locker=Lock("uploads")
    threads = set()


    # DATE TO DAYS-SINCE-2000
    day = Date(string2datetime(p[7:17], format="%Y-%m-%d"))
    day_num = int((day - REFERENCE_DATE) / DAY)
    day_url = settings.source.url + p
    key0 = unicode(day_num) + ".0"

    if day < EARLIEST_CONSIDERATION_DATE or Date.today() <= day:
        # OUT OF BOUNDS, TODAY IS NOT COMPLETE
        return

    Log.note("Consider #{{num}}: {{url}}", url=day_url, num=day_num)

    destination = s3.Bucket(settings.destination)
    notify = Queue(settings=settings.notify)

    if force:
        with suppress_exception:
            destination.delete_key(key0)
    else:
        # CHECK TO SEE IF THIS DAY WAS DONE
        if destination.get_meta(key0):
            return

    Log.note("Processing {{url}}", url=day_url)
    day_etl = Dict(
        id=day_num,
        url=day_url,
        timestamp=Date.now(),
        type="join"
    )
    tasks = get_all_tasks(day_url)
    first = None
    for group_number, ts in jx.groupby(tasks, size=100):
        if DEBUG:
            Log.note("Processing block {{num}}", num=group_number)
        parsed = []

        group_etl = Dict(
            id=group_number,
            source=day_etl,
            type="join",
            timestamp=Date.now()
        )
        for row_number, d in enumerate(ts):
            row_etl = Dict(
                id=row_number,
                source=group_etl,
                type="join"
            )
            try:
                d.etl = row_etl
                parsed.append(convert.value2json(d))
            except Exception, e:
                d = {"etl": row_etl}
                parsed.append(convert.value2json(d))
                Log.warning("problem in {{path}}", path=day_url, cause=e)

        if group_number == 0:
            # WRITE THE FIRST BLOCK (BLOCK 0) LAST
            first = parsed
            continue

        key = unicode(day_num) + "." + unicode(group_number)

        def upload(key, lines, please_stop):
            destination.write_lines(key=key, lines=lines)
            notify.add({"key": key, "bucket": destination.name, "timestamp": Date.now()})
            with locker:
                threads.remove(Thread.current())

        while True:
            with locker:
                if len(threads) <= 20:
                    break
            Thread.sleep(seconds=0.1)

        thread = Thread.run("upload " + key, upload, key, parsed)
        with locker:
            threads.add(thread)

    if first == None:
        Log.error("How did this happen?")

    # WRITE FIRST BLOCK
    key0 = unicode(day_num) + ".0"
    destination.write_lines(key=key0, lines=first)
    notify.add({"key": key0, "bucket": destination.name, "timestamp": Date.now()})

    # CONFIRM IT WAS WRITTEN
    if not destination.get_meta(key0):
        Log.error("Key zero is missing?!")


def get_all_logs(url):
    # GET LIST OF LOGS
    paths = []
    response = http.get(url)
    try:
        for line in response.all_lines:
            # <tr><td valign="top"><img src="/icons/compressed.gif" alt="[   ]"></td><td><a href="builds-2015-09-20.js.gz">builds-2015-09-20.js.gz</a></td><td align="right">20-Sep-2015 19:00  </td><td align="right">6.9M</td><td>&nbsp;</td></tr>
            filename = strings.between(line, '</td><td><a href=\"', '">')
            if filename and filename.startswith("builds-2") and not filename.endswith(".tmp"):  # ONLY INTERESTED IN DAILY SUMMARY FILES (eg builds-2015-09-20.js.gz)
                paths.append(filename)
        paths = jx.reverse(jx.sort(paths))
        return paths
    finally:
        response.close()


def get_all_tasks(url):
    """
    RETURN ITERATOR OF ALL `builds` IN THE BUILDBOT JSON LOG
    """
    _file = TemporaryFile()
    with Timer("copy json log to local file"):
        response = http.get(url)
        _stream = response.raw
        size = 0
        while True:
            chunk = _stream.read(http.MIN_READ_SIZE)
            if not chunk:
                break
            size += len(chunk)
            _file.write(chunk)
        _file.seek(0)
    Log.note("File is {{num}} bytes", num=size)

    return stream.parse(
        scompressed2ibytes(_file),
        "builds",
        expected_vars=["builds"]
    )


def main():
    try:
        with Explanation("ETL"):
            settings = startup.read_settings()
            constants.set(settings.constants)
            Log.start(settings.debug)

            parse_to_s3(settings)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
