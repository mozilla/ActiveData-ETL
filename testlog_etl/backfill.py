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
from pyLibrary.dot import coalesce, wrap
from pyLibrary.env import elasticsearch
from pyLibrary.maths import Math
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer


def diff(settings, please_stop=None):
    if not settings.elasticsearch.id_field:
        Log.error("Expecting an `id_field` property")
    if settings.range.min == None:
        settings.range.min = coalesce(settings.start, 0)

    #SHOULD WE PUSH?
    work_queue = aws.Queue(settings=settings.work_queue)
    if not settings.no_checks and len(work_queue) > 200:
        Log.alert("{{queue}} queue has {{num}} elements, adding more is not a good idea", queue=work_queue.name, num=len(work_queue))
        return

    # EVERYTHING FROM ELASTICSEARCH
    es = elasticsearch.Index(settings.elasticsearch)
    source_bucket = s3.Bucket(settings.source)

    in_es = get_all_in_es(es, settings.range, settings.elasticsearch.id_field)
    in_range = None
    if settings.range:
        max_in_es = Math.MAX(in_es)
        in_range = set(range(coalesce(settings.range.min, 0), coalesce(settings.range.max, max_in_es, 1000000)))
        in_es -= in_range

    remaining_in_s3 = get_all_s3(in_es, in_range, settings)

    # IGNORE THE 500 MOST RECENT BLOCKS, BECAUSE THEY ARE PROBABLY NOT DONE
    if settings.no_checks:
        remaining_in_s3 = remaining_in_s3[:coalesce(settings.limit, 1000):]
    else:
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

    for i, p in enumerate(remaining_in_s3):
        all_keys = source_bucket.keys(unicode(p) + ":")
        Log.note("{{count}}. {{key}} has {{num}} subkeys, added to {{queue}}", count=i, key=p, num=len(all_keys), queue=work_queue.name)
        work_queue.extend([
            {
                "key": k,
                "bucket": source_bucket.name,
                "destination": settings.destination,
                "timestamp": Date.now()
            }
            for k in all_keys
        ])


def get_all_in_es(es, in_range, field):
    in_es = set()
    es_query = wrap({
        "aggs": {
            "_filter": {
                "filter": {"and":[
                    {"match_all": {}}
                ]},
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
    if in_range:
        _filter = es_query.aggs._filter.filter["and"]
        if in_range.min:
            _filter.append({"range": {field: {"gte": in_range.min}}})
        if in_range.max:
            _filter.append({"range": {field: {"lt": in_range.max}}})


    result = es.search(es_query)

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

def get_all_s3(in_es, in_range, settings):
    # EVERYTHING FROM S3
    bucket = s3.Bucket(settings.source)
    with Timer("Scanning S3 bucket {{bucket}}", {"bucket": bucket.name}):
        prefixes = [p.name.rstrip(":") for p in bucket.list(prefix="", delimiter=":")]

    in_s3 = []
    for i, q in enumerate(prefixes):
        if i % 1000 == 0:
            Log.note("Scrubbed {{p|percent(decimal=1)}}", p=i / len(prefixes))
        try:
            p = int(q)
            if in_range and p not in in_range:
                continue
            if p in in_es:
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
