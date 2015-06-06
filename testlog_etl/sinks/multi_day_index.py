# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary import strings, convert
from pyLibrary.aws.s3 import strip_extension
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap
from pyLibrary.env import elasticsearch
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import WEEK
from testlog_etl import key2etl, etl2path


# NEW_INDEX_INTERVAL = WEEK


class MultiDayIndex(object):
    """
    MIMIC THE elasticsearch.Index, WITH EXTRA keys() FUNCTION
    AND THREADED QUEUE AND SPLIT DATA BY
    """
    def __init__(self, settings, queue_size=10000):
        self.settings = settings
        self.queue_size = queue_size
        self.indicies = {}  # MAP DATE (AS UNIX TIMESTAMP) TO INDEX

        es = elasticsearch.Cluster(self.settings).get_or_create_index(settings=self.settings)
        es.add_alias(self.settings.index)
        es.set_refresh_interval(seconds=60 * 60)
        self.queue = es.threaded_queue(max_size=self.queue_size, batch_size=5000, silent=False)
        self.es = elasticsearch.Alias(alias=settings.index, settings=settings)
        #FORCE AT LEAST ONE INDEX TO EXIST
        dummy = wrap({"build": {"date": Date.now().unix}})

    # def _get_queue(self, d):
    #     date = Date(coalesce(d.build.date, d.run.timestamp)).floor(NEW_INDEX_INTERVAL)
    #     if not date:
    #         Log.error("Can not get date from document")
    #     name = self.settings.index + "_" + date.format("%Y-%m-%d")
    #     uid = date.unix
    #
    #     queue = self.indicies.get(uid)
    #     if queue==None:
    #         es = elasticsearch.Cluster(self.settings).get_or_create_index(index=name, settings=self.settings)
    #         es.add_alias(self.settings.index)
    #         es.set_refresh_interval(seconds=60 * 60)
    #         queue = es.threaded_queue(max_size=self.queue_size, batch_size=5000, silent=False)
    #         self.indicies[uid] = queue
    #
    #     return queue

    def __getattr__(self, item):
        return getattr(self.es, item)

    # ADD keys() SO ETL LOOP CAN FIND WHAT'S GETTING REPLACED
    def keys(self, prefix=None):
        path = qb.reverse(etl2path(key2etl(prefix)))

        result = self.es.search({
            "fields": ["_id"],
            "query": {
                "filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [{"term": {"etl" + (".source" * i) + ".id": v}} for i, v in enumerate(path)]}
                }
            }
        })

        if result.hits.hits:
            return set(result.hits.hits._id)
        else:
            return set()

    def extend(self, documents):
        for d in wrap(documents):
            self.queue.add(d)

    def add(self, doc):
        self.queue.add(doc)

    def delete(self, filter):
        self.es.delete(filter)

    def copy(self, keys, source, sample_only_filter=None, sample_size=None):
        num_keys = 0
        for key in keys:
            queue = None  # PUT THE WHOLE FILE INTO SAME INDEX
            try:
                for rownum, line in enumerate(source.read_lines(strip_extension(key))):
                    if rownum == 0:
                        value = convert.json2value(line)
                        value = _fix(value)
                        row = {"id": value._id, "value": value}
                        if sample_only_filter and Random.int(int(1.0/coalesce(sample_size, 0.01))) != 0 and qb.filter([value], sample_only_filter):
                            # INDEX etl.id==0, BUT NO MORE
                            if value.etl.id != 0:
                                Log.error("Expecting etl.id==0")
                            num_keys += 1
                            self.queue.add(row)
                            break
                    else:
                        #FAST
                        #strings.between(line, "_id\": \"", "\"")  # AVOID DECODING JSON
                        # row = {"id": _id, "json": line}

                        #SLOW
                        value = convert.json2value(line)
                        value = _fix(value)
                        _id = value._id
                        row = {"id": _id, "value": value}
                    num_keys += 1
                    self.queue.add(row)
            except Exception, e:
                Log.warning("Could not get queue for {{key}}", key=key, cause=e)
        return num_keys

def _fix(value):
    if not value.build.revision12:
        value.build.revision12 = value.build.revision[0:12]
    return value
