# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import nvl, wrap
from pyLibrary.env import elasticsearch
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import WEEK
from testlog_etl import key2etl, etl2path


NEW_INDEX_INTERVAL = WEEK


class MultiDayIndex(object):
    """
    MIMIC THE elasticsearch.Index, WITH EXTRA keys() FUNCTION
    AND THREADED QUEUE AND SPLIT DATA BY
    """
    def __init__(self, settings):
        self.settings = settings
        self.indicies = {}  # MAP DATE (AS UNIX TIMESTAMP) TO INDEX
        self.es = elasticsearch.Alias(alias=settings.index, settings=settings)
        #ENSURE WE HAVE ONE INDEX
        dummy = wrap({"value": {"build": {"date": Date.now().unix}}})
        self._get_queue(dummy)

    def _get_queue(self, d):
        date = Date(nvl(d.value.build.date, d.value.run.timestamp)).floor(NEW_INDEX_INTERVAL)
        name = self.settings.index + "_" + date.format("%Y-%m-%d")
        uid = date.unix

        queue = self.indicies.get(uid)
        if queue==None:
            es = elasticsearch.Cluster(self.settings).get_or_create_index(index=name, settings=self.settings)
            es.add_alias(self.settings.index)
            es.set_refresh_interval(seconds=60 * 60)
            queue = es.threaded_queue(max_size=2000, batch_size=1000, silent=False)
            self.indicies[uid] = queue

        return queue


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

        return set(result.hits.hits.fields._id)

    def extend(self, documents):
        for d in wrap(documents):
            try:
                queue = self._get_queue(d)
                queue.add(d)
            except Exception, e:
                Log.error("Can not decide on index by build.date: {{doc|json}}", {"doc": d.value})

    def add(self, doc):
        d = wrap(doc)
        queue = self._get_queue(Date(nvl(d.value.build.date, d.value.run.timestamp)))
        queue.add(doc)
