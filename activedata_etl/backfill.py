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

import jx_elasticsearch
from jx_base.expressions import TRUE
from jx_python import jx
from mo_dots import coalesce
from mo_future import text_type
from mo_logs import Log
from mo_logs import startup, constants
from mo_logs.exceptions import suppress_exception
from mo_math import MAX, MIN, ceiling, log10
from mo_times.dates import Date
from mo_times.timer import Timer
from pyLibrary import aws
from pyLibrary.aws import s3
from pyLibrary.env.git import get_remote_revision


def diff(settings, please_stop=None):
    if not settings.elasticsearch.id_field:
        Log.error("Expecting an `id_field` property")
    settings.range.min = coalesce(settings.range.min, settings.start, 0)

    # SHOULD WE PUSH?
    work_queue = aws.Queue(kwargs=settings.work_queue)
    if not settings.no_checks and len(work_queue) > 100:
        Log.alert("{{queue}} queue has {{num}} elements, adding more is not a good idea", queue=work_queue.name, num=len(work_queue))
        return

    esq = jx_elasticsearch.new_instance(settings.elasticsearch)
    source_bucket = s3.Bucket(settings.source)

    if settings.git:
        rev = get_remote_revision(settings.git.url, settings.git.branch)
        es_filter = {"prefix": {"etl.revision": rev[0:12]}}
    else:
        es_filter = coalesce(settings.es_filter, {"match_all": {}})

    # EVERYTHING FROM ELASTICSEARCH
    in_es = get_all_in_es(esq, settings.range, es_filter, settings.elasticsearch.id_field)
    in_range = None
    if settings.range:
        max_in_es = MAX(in_es)
        _min = coalesce(settings.range.min, 0)
        _max = coalesce(
            settings.range.max,
            coalesce(settings.limit, 0) + max_in_es + 1,
            _min + coalesce(settings.limit, 1000000)
        )
        in_range = set(range(_min, _max))
        in_es &= in_range

    remaining_in_s3 = get_all_s3(in_es, in_range, settings)

    if not remaining_in_s3:
        Log.note("Nothing to insert into ES")
        return

    Log.note(
        "Queueing {{num}} keys (from {{min}} to {{max}}) for insertion to {{queue}}",
        num=len(remaining_in_s3),
        min=MIN(remaining_in_s3),
        max=MAX(remaining_in_s3),
        queue=work_queue.name
    )

    source_prefix = coalesce(settings.source.prefix, "")
    for i, p in enumerate(remaining_in_s3):
        all_keys = source_bucket.keys(source_prefix + text_type(p))
        Log.note("{{count}}. {{key}} has {{num}} subkeys, added to {{queue}}", count=i, key=p, num=len(all_keys), queue=work_queue.name)
        with Timer("insert into aws sqs", silent=len(all_keys) == 1):
            work_queue.extend([
                {
                    "key": k,
                    "bucket": source_bucket.name,
                    "destination": settings.destination,
                    "timestamp": Date.now()
                }
                for k in all_keys
            ])


def get_all_in_es(esq, in_range, es_filter, field):
    if es_filter == None:
        es_filter = TRUE

    range_filter = []
    if in_range:
        if in_range.min:
            range_filter.append({"gte": {field: in_range.min}})
        if in_range.max:
            range_filter.append({"lt": {field: in_range.max}})

    result = esq.query({
        "from": esq.es.settings.alias,
        "edges": {"name": "value", "value": field},
        "where": {"and": [es_filter] + range_filter},
        "limit": 100000,
        "format": "list"
    })

    in_es = set()
    for rec in result.data:
        with suppress_exception:
            in_es.add(int(rec.value))

    Log.note(
        "got {{num}} from {{index}}",
        num=len(in_es),
        index=esq.settings.index
    )

    return in_es


def get_all_s3(in_es, in_range, settings):
    in_s3 = []
    min_range = coalesce(MIN(in_range), 0)
    bucket = s3.Bucket(settings.source)
    limit = coalesce(settings.limit, 1000)
    max_allowed = MAX([settings.range.max, MAX(in_es)])
    extra_digits = ceiling(log10(MIN([max_allowed-settings.range.min, limit])))
    source_prefix = coalesce(settings.source.prefix, "")

    prefix = text_type(max(in_range - in_es))[:-extra_digits]
    prefix_max = int(prefix + ("999999999999"[:extra_digits]))
    while prefix != "0" and len(in_s3) < limit and min_range <= prefix_max:
        # EVERYTHING FROM S3
        with Timer(
            "Scanning S3 bucket {{bucket}} with prefix {{prefix|quote}}",
            {"bucket": bucket.name, "prefix": source_prefix+prefix}
        ):
            prefixes = set()
            for p in bucket.list(prefix=source_prefix+prefix, delimiter=":"):
                if p.name.startswith("bb.") or p.name.startswith("tc."):
                    pp = p.name.split(":")[0].split(".")[1]
                else:
                    pp = p.name.split(":")[0].split(".")[0]
                prefixes.add(pp)
            prefixes = list(prefixes)

        for i, q in enumerate(prefixes):
            if i % 1000 == 0:
                Log.note("Scrubbed {{p|percent(decimal=1)}}", p=i / len(prefixes))
            try:
                p = int(q)
                if in_range and p not in in_range:
                    continue
                if p in in_es:
                    continue
                # if p >= max_allowed:
                #     continue

                in_s3.append(p)
            except Exception as e:
                Log.note("delete key? {{key|quote}}", key=q)

        if prefix == "":
            break
        prefix = text_type(int(prefix) - 1)
        prefix_max = int(prefix + ("999999999999"[:extra_digits]))

    in_s3 = jx.reverse(jx.sort(in_s3))[:limit:]
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
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
