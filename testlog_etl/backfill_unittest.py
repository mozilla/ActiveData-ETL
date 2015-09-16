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
from pyLibrary.aws import s3
from pyLibrary.aws.s3 import strip_extension
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce
from pyLibrary.env import elasticsearch
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.times.timer import Timer


def diff(settings, please_stop=None):
    #SHOULD WE PUSH?
    work_queue = aws.Queue(settings=settings.work_queue)
    if not settings.no_checks and len(work_queue) > 100:
        Log.alert("Index queue has {{num}} elements, adding more is not a good idea", num=len(work_queue))
        return

    # EVERYTHING FROM ELASTICSEARCH
    es = elasticsearch.Index(settings.destination)

    in_es = get_all_in_es(es)
    in_range = None
    if settings.range:
        settings.limit = 1000000000  # SOME BIG NUMBER
        max_in_es = max(*in_es)
        in_range = set(range(coalesce(settings.range.min, 0), coalesce(settings.range.max, max_in_es)))
        in_es -= in_range


    in_s3 = get_all_s3(in_es, in_range, settings)

    # IGNORE THE 500 MOST RECENT BLOCKS, BECAUSE THEY ARE PROBABLY NOT DONE
    if not settings.no_checks:
        in_s3 = in_s3[500:500 + coalesce(settings.limit, 1000):]

    if not in_s3:
        Log.note("Nothing to insert into ES")
        return

    Log.note(
        "Queueing {{num}} keys (from {{min}} to {{max}}) for insertion to {{queue}}",
        num=len(in_s3),
        min=Math.MIN(in_s3),
        max=Math.MAX(in_s3),
        queue=work_queue.name
    )
    work_queue.extend(in_s3)


def get_all_in_es(es):
    in_es = set()

    result = es.search({
        "aggs": {
            "_match": {
                "terms": {
                    "field": "etl.source.source.id",
                    "size": 200000
                }
            }
        },
        "size": 0
    })

    good_es = []
    for k in result.aggregations._match.buckets.key:
        try:
            good_es.append(int(k))
        except Exception, e:
            pass

    Log.note(
        "got {{num}} from {{index}}",
        num=len(good_es),
        index=es.settings.index
    )
    in_es |= set(good_es)

    return in_es

def get_all_s3(in_es, in_range, settings):
    # EVERYTHING FROM S3
    bucket = s3.Bucket(settings.source)
    with Timer("Scanning S3"):
        prefixes = [p.name.rstrip(":") for p in bucket.list(prefix="", delimiter=":")]

    in_s3 = []
    for i, p in enumerate(prefixes):
        if i % 1000 == 0:
            Log.note("Scrubbed {{p|percent(decimal=1)}}", p=i / len(prefixes))
        try:
            if int(p) not in in_es:
                if not in_range or int(p) in in_range:
                    in_s3.append(int(p))
            else:
                pass
        except Exception, _:
            Log.note("delete key {{key}}", key= p)
            bucket.delete_key(strip_extension(p))
    in_s3 = qb.reverse(qb.sort(in_s3))
    return in_s3


def main():
    """
    RE INDEX DATA FROM S3
    IF THE ETL IS GOOD, AND YOU JUST NEED TO FILL ES, USE THIS
    """
    try:
        settings = startup.read_settings(defs=[
            {
                "name": ["--id"],
                "help": "id (prefix, really) to process",
                "type": str,
                "dest": "id",
                "required": False
            }
        ])
        constants.set(settings.constants)
        Log.start(settings.debug)

        diff(settings)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
