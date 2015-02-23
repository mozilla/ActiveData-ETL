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

import json
from pyLibrary import aws
from pyLibrary import convert
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import nvl

from pyLibrary import jsons
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer




def find_missing(settings):
    """
    FIND ANY RECORDS IN THE DEV PULSE LOG THAT ARE NOT FOUND IN STAGING,
    ADD TO STAGING
    """
    min_date = Date(nvl(settings.param.min_date, Date.MIN))
    max_date = Date(nvl(settings.param.max_date, Date.MAX))

    dev = aws.s3.Bucket(settings.dev)
    stage = aws.s3.Bucket(settings.stage)
    # stage.bucket.delete_key('11890:1134372.json')

    # FIND ALL KEYS DURING LOST TIME IN DEV
    dev_records, _ = extract_records(dev, min_date, max_date, None)
    # FIND ALL KEYS DURING LOST TIME IN STAGING
    stage_records, last = extract_records(stage, min_date, max_date, settings.param.dest_key)

    # FIND DIFFERENCE
    missing = set(dev_records.keys()) - set(stage_records.keys())
    net_new = [dev_records[k] for k in missing]

    if net_new:
        # FIND KEY IN STAGING, ADD ALL EXTRAS TO IT
        data = stage.read(last.key)
        data = data + "\n" + ("\n".join(net_new))

        with Timer("Update {{key}}", {"key": last.key}):
            stage.write(last.key, data)


def extract_records(bucket, min_date, max_date, dest_key):
    records = {}
    metas = bucket.metas()
    filtered = qb.run({
        "from": metas,
        "where": {"or":[
            {"range": {"last_modified": {"gte": min_date, "lt": max_date}}},
            {"term": {"key": dest_key}} if dest_key is not None else False
        ]},
        "sort": "last_modified"
    })
    for meta in filtered:
        try:
            Log.note("Read {{key}} {{timestamp}}", {"key": meta.key, "timestamp": meta.last_modified})
            rs = bucket.read(meta.key)
            for r in rs.replace("}{", "}\n{").split("\n"):
                try:
                    value = convert.json2value(r)
                except Exception, e:
                    Log.error("can not decode json", e)
                if not value.locale:
                    continue
                uid = convert.bytes2sha1(json_encode(value))
                records[uid] = r
        except Exception, e:
            Log.error("Problem with {{key}} {{timestamp}}", {"key": meta.key, "timestamp": meta.last_modified})
    return records, filtered.last()


json_encoder = json.JSONEncoder(
    skipkeys=False,
    ensure_ascii=True,
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=None,
    encoding='utf8',
    default=None,
    sort_keys=True   # <-- IMPORTANT!  sort_keys==True
)


def json_encode(value):
    """
    FOR PUTTING JSON INTO DATABASE (sort_keys=True)
    dicts CAN BE USED AS KEYS
    """
    return json_encoder.encode(jsons.scrub(value))


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        find_missing(settings)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
