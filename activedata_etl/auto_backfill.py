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

from pyLibrary.thread.threads import Thread

from activedata_etl import key2etl
from pyLibrary.times.durations import Duration

from pyLibrary import aws
from pyLibrary.aws import s3
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.exceptions import suppress_exception
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap, Dict
from pyLibrary.env import elasticsearch, http
from pyLibrary.env.git import get_remote_revision
from pyLibrary.maths import Math
from pyLibrary.queries import jx
from pyLibrary.queries.expressions import jx_expression
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer

ACTIVE_DATA = "http://activedata.allizom.org/query"




def get_all_in_es(es, in_range, es_filter, field):
    if es_filter==None:
        es_filter = {"match_all": {}}

    in_es = set()
    es_query = wrap({
        "aggs": {
            "_filter": {
                "filter": {"and": [
                    es_filter
                ]},
                "aggs": {
                    "_match": {
                        "terms": {
                            # "field": field,
                            "script": jx_expression({"string": field}).to_ruby(),
                            "size": 200000
                        }
                    }
                }
            }
        },
        "size": 0
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
        with suppress_exception:
            good_es.append(int(k))

    Log.note(
        "got {{num}} from {{index}}",
        num=len(good_es),
        index=es.settings.index
    )
    in_es |= set(good_es)

    return in_es


def get_all_s3(in_es, in_range, source_prefix, settings):
    in_s3 = []
    min_range = coalesce(Math.MIN(in_range), 0)
    bucket = s3.Bucket(settings.source)
    max_allowed = Math.MAX([Math.MAX(in_range), Math.MAX(in_es)])
    min_allowed = Math.MIN([Math.MIN(in_range), Math.MIN(in_es)])
    extra_digits = Math.ceiling(Math.log10(max_allowed-min_allowed))

    prefix = unicode(max(in_range - in_es))[:-extra_digits]
    prefix_max = int(prefix + ("999999999999"[:extra_digits]))
    while prefix != "0" and min_range <= prefix_max:
        # EVERYTHING FROM S3
        with Timer(
            "Scanning S3 bucket {{bucket}} with prefix {{prefix|quote}}",
            {"bucket": bucket.name, "prefix": source_prefix + "." + prefix}
        ):
            prefixes = set()
            for p in bucket.list(prefix=source_prefix + "." + prefix, delimiter=":"):
                pp = p.name.split(":")[0].split(".")[1]
                prefixes.add(pp)
            prefixes = list(prefixes)

        for i, q in enumerate(prefixes):
            try:
                p = int(q)
                if in_range and p not in in_range:
                    continue
                if p in in_es:
                    continue
                # if p >= max_allowed:
                #     continue

                in_s3.append(p)
            except Exception, e:
                Log.note("delete key? {{key|quote}}", key=q)

        if prefix == "":
            break
        prefix = unicode(int(prefix) - 1)
        prefix_max = int(prefix + ("999999999999"[:extra_digits]))

    in_s3 = jx.reverse(jx.sort(in_s3))
    return in_s3


def backfill_recent(settings, index_queue, please_stop):
    max_backfill = Math.round(settings.batch_size / 10, decimal=0)
    max_duration = Duration(settings.rollover.max)
    interval = Duration(settings.rollover.interval)
    oldest_backfill = (Date.now() - max_duration).floor(interval).unix
    date_field = settings.rollover.field

    # WHAT IS THE KEY FORMAT?
    key_format = settings.source.key_format
    example_etl = key2etl(key_format)
    source_depth = 1
    temp = example_etl
    while temp.source:
        temp = temp.source
        source_depth += 1
    main_depth = source_depth - 2

    def etl_source(depth_):
        return "etl" + "".join([".source"] * depth_)

    source_field = etl_source(source_depth) + ".code"
    main_id = etl_source(main_depth) + ".id"

    def discriminate(source, min_id, max_id):
        return [
           {"eq": {etl_source(i) + ".id": 0}}
           for i in range(main_depth)
        ] + filter_range(source, min_id, max_id)

    def filter_range(source, min_id, max_id):
        return [
            {"gte": {main_id: min_id}},
            {"lte": {main_id: max_id}},
            {"eq": {source_field: source}}
        ]

    def bisect(source, min_id, max_id):
        result = http.post_json(ACTIVE_DATA, json={
            "from": settings.elasticsearch.index,
            "select": [
                {
                    "name": "min",
                    "value": main_id,
                    "aggregate": "min"
                },
                {
                    "name": "max",
                    "value": main_id,
                    "aggregate": "max"
                },
                {
                    "aggregate": "count"
                },
                {
                    "name": "youngest",
                    "value": date_field,
                    "aggregate": "max"
                }
            ],
            "where": {"and": discriminate(source, min_id, max_id)},
            "format": "list"
        })
        min_id = result.data.min
        max_id = result.data.max
        num = max_id - min_id + 1

        if result.data.youngest < oldest_backfill:
            # DATA IS TOO OLD TO BOTHER WITH
            pass
        elif num > result.data.count:
            if num > max_backfill:
                # BISECT AND RETRY
                mid_id = int(round((max_id + min_id) / 2))
                bisect(source, mid_id, max_id)  # DO THE HIGHER VALUES FIRST
                bisect(source, min_id, mid_id)
            else:
                # LOAD HOLES
                fill_big_holes(source, min_id, max_id)
        else:
            # GOOD! LOOK FOR TINY HOLES
            Log.note("{{min}} to {{max}} is dense, look for small holes", min=min_id, max=max_id)
            fill_tiny_holes(source, min_id, max_id, main_depth - 1)

    def fill_big_holes(source, min_id, max_id):
        result = http.post_json(ACTIVE_DATA, json={
            "from": settings.elasticsearch.index,
            "select": main_id,
            "where": {"and": discriminate(source, min_id, max_id)},
            "sort": "_id",
            "format": "list"
        })
        in_range = set(range(int(min_id), int(max_id) + 1, 1))
        in_es = set(result.data)
        keys = get_all_s3(in_es, in_range, source, settings)

        for k in keys:
            now = Date.now()
            index_queue.add({
                "bucket": settings.source.bucket,
                "key": source + "." + unicode(k),
                "timestamp": now.unix,
                "date/time": now.format()
            })

    def fill_tiny_holes(source, min_id, max_id, depth):
        pass
        # second =  [
        #    {"eq": {etl_source(i) + ".id": 0}}
        #    for i in range(depth)
        # ] + filter_range(source, min_id, max_id)
        #
        # result = http.post_json(ACTIVE_DATA, json={
        #     "from": settings.elasticsearch.index,
        #     "select": {"aggregate": "count"},
        #     "where": {"and": filter_range(source, min_id, max_id)}
        # })
        # if result.data.count < max_backfill:
        #     result = http.post_json(ACTIVE_DATA, json={
        #         "from": settings.elasticsearch.index,
        #         "select": "_id",
        #         "where": {"and": second},
        #         "format": "list"
        #     })
        #     in_es = set(result.data)
        #     in_range = set()




    bisect("bb", 0, 10000000000)
    bisect("tc", 0, 10000000000)



    # WHAT IS IN ES NOW, AND WHAT IS THE DATE RANGE? CAN WE ESTIMATE S3 RANGE?
    # DO A BACKWARDS SCAN OF S3?

    # LOOK FOR BLATANT HOLES
    #



    # FOR EACH LEVEL OF ETL KEY, ENSURE THE WHOLE RANGE MATCHES S3
    # BISECT UNTIL WE HAVE A MANAGEABLE CHUNK

    # LOOK FOR HOLES IN A BATCH
    # FIND THE OLDEST DATA THAT IS STILL ALLOWED TO BE BACK-FILLED
    # FIND THE MOST RECENT
    # ADD TO THE INDEX QUEUE


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
        queue = aws.Queue(settings.work_queue)

        threads = [
            Thread.run(w.name, backfill_recent, w, queue)
            for w in settings.workers[0:1:]
        ]

        for t in threads:
            Thread.join(t)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
