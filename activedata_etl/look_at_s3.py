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
from pyLibrary import convert

from pyLibrary import aws
from pyLibrary.aws.s3 import Bucket
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log

from pyLibrary.queries import jx
from pyLibrary.times.timer import Timer


def list_s3(settings, filter):
    """
    LIST THE KEYS AND TIMESTAMPS FOUND IN AN S3 BUCKET
    """
    bucket = Bucket(settings)
    with Timer("get all metadata from {{bucket}}", {"bucket":bucket.name}):
        prefix = filter.prefix.key  # WE MAY BE LUCKY AND THIS IS THE ONLY FILTER
        metas = bucket.metas(prefix=prefix)

    filtered = jx.run({
        "from": metas,
        "where": filter,
        "sort": "last_modified"
    })
    for meta in filtered:
        Log.note("Read {{key}} {{timestamp}}", key=meta.key, timestamp= meta.last_modified)

    if len(filtered)==1:
        Log.note("{{content}}", content=bucket.read(filtered[0].key))


def list_queue(settings, num=10):
    queue = aws.Queue(settings)
    for i in range(num):
        content = queue.pop()
        Log.note("{{content}}",  content=content)
    queue.rollback()


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        list_s3(settings.source, settings.filter)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
