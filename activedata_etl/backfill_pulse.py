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

from pyLibrary import aws
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.env import elasticsearch
from pyLibrary.queries import jx
from activedata_etl.transforms import pulse_block_to_es


def backfill(settings):

    source = aws.s3.Bucket(settings=settings.source)
    destination = elasticsearch.Cluster(settings=settings.destination).get_or_create_index(settings=settings.destination)

    keep_trying = True
    while keep_trying:
        try:
            all_keys = source.keys()
            keep_trying=False
        except Exception, e:
            Log.warning("problem", e)

    # all_keys = set()
    # for i in range(20, 97, 1):
    #     all_keys |= source.keys(prefix=unicode(i))

    for k in jx.sort(all_keys):
        try:
            pulse_block_to_es.process(k, source.get_key(k), destination)
        except Exception, e:
            Log.warning("Problem with {{key}}", key=k, cause=e)


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        backfill(settings)
    except Exception, e:
        Log.error("Problem with backfill", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
