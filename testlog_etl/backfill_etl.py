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
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce
from pyLibrary.env import elasticsearch
from pyLibrary.maths import Math
from pyLibrary.queries import qb


def diff(settings, please_stop=None):
    if not settings.id_field:
        Log.error("Expecting an `id_field` property")

    #SHOULD WE PUSH?
    work_queue = aws.Queue(settings=settings.work_queue)
    if not settings.no_checks and len(work_queue) > 100:
        Log.alert("Index queue has {{num}} elements, adding more is not a good idea", num=len(work_queue))
        return

    # EVERYTHING FROM ELASTICSEARCH
    es = elasticsearch.Index(settings.destination)
    source_bucket = s3.Bucket(settings.source)

    in_es = get_all_in_es(es, settings.id_field, settings.start)
    remaining_in_s3 = get_all_s3(in_es, source_bucket, settings.start)

    # IGNORE THE 500 MOST RECENT BLOCKS, BECAUSE THEY ARE PROBABLY NOT DONE
    if not settings.no_checks:
        remaining_in_s3 = remaining_in_s3[500:500 + coalesce(settings.limit, 1000):]

    if not remaining_in_s3:
        Log.note("Nothing to insert into ES")
        return

    Log.note(
        "Queueing {{num}} keys (from {{min}} to {{max}}) for insertion to {{queue}}",
        num=len(remaining_in_s3),
        min=Math.MIN(remaining_in_s3),
        max=Math.MAX(remaining_in_s3),
        queue=work_queue.name
    )

    for p in remaining_in_s3:
        all_keys = source_bucket.keys(unicode(p) + ":")
        work_queue.extend([{"key": k, "bucket": source_bucket.name} for k in all_keys])


def get_all_in_es(es, field, start=0):
    in_es = set()

    result = es.search({
        "aggs": {
            "_filter": {
                "filter": {"range": {field: {"gte": start}}},
                "aggs": {
                    "_match": {
                        "terms": {
                            "field": field,
                            "size": 200000
                        }

                    }
                }
            }
        }
    })

    good_es = []
    for k in result.aggregations._filter._match.buckets.key:
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

def get_all_s3(in_es, source_bucket, start=0):
    Log.note("Scanning S3")
    prefixes = [p.name.rstrip(":") for p in source_bucket.list(prefix="", delimiter=":")]
    in_s3 = []
    for i, q in enumerate(prefixes):
        if i % 1000 == 0:
            Log.note("Scrubbed {{p|percent(decimal=1)}}", p=i / len(prefixes))
        try:
            p = int(q)
            if p in in_es or p < start:
                continue

            in_s3.append(p)
        except Exception:
            Log.note("delete key? {{key|quote}}", key=q)
            # source_bucket.delete_key(strip_extension(q))
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
