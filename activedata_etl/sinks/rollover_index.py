# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary import convert, strings
from pyLibrary.aws.s3 import strip_extension
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import coalesce, wrap, Null
from pyLibrary.env import elasticsearch
from pyLibrary.maths.randoms import Random
from pyLibrary.meta import use_settings
from pyLibrary.queries import jx
from activedata_etl import key2etl, etl2path
from pyLibrary.times.dates import Date, unicode2Date
from pyLibrary.times.durations import Duration


class RolloverIndex(object):
    """
    MIMIC THE elasticsearch.Index, WITH EXTRA keys() FUNCTION
    AND THREADED QUEUE AND SPLIT DATA BY
    """
    @use_settings
    def __init__(self, rollover_field, rollover_interval, queue_size=10000, batch_size=5000, settings=None):
        """
        :param rollover_field: the FIELD with a timestamp to use for determining which index to push to
        :param rollover_interval: duration between roll-over to new index
        :param queue_size: number of documents to queue in memory
        :param batch_size: number of documents to push at once
        :param settings: plus additional ES settings
        :return:
        """
        self.settings = settings
        self.rollover_field = jx.get(rollover_field)
        self.rollover_interval = self.settings.rollover_interval = Duration(settings.rollover_interval)
        self.known_queues = {}  # MAP DATE TO INDEX
        self.cluster = elasticsearch.Cluster(self.settings)

    def __getattr__(self, item):
        return getattr(self.cluster, item)
        # Log.error("Not supported")

    def _get_queue(self, row):
        row = wrap(row)
        if row.json:
            row.value, row.json = convert.json2value(row.json), None
        timestamp = Date(self.rollover_field(wrap(row).value))
        if timestamp == None:
            return Null

        rounded_timestamp = timestamp.floor(self.rollover_interval)
        queue = self.known_queues.get(rounded_timestamp.unix)
        if queue == None:
            candidates = jx.run({
                "from": self.cluster.get_aliases(),
                "where": {"regex": {"index": self.settings.index + "\d\d\d\d\d\d\d\d_\d\d\d\d\d\d"}},
                "sort": "index"
            })
            best = None
            for c in candidates:
                c = wrap(c)
                c.date = unicode2Date(c.index[-15:], elasticsearch.INDEX_DATE_FORMAT)
                if timestamp > c.date:
                    best = c
            if not best or rounded_timestamp > best.date:
                if rounded_timestamp < wrap(candidates[-1]).date:
                    es = elasticsearch.Index(read_only=False, alias=best.alias, index=best.index, settings=self.settings)
                else:
                    try:
                        es = self.cluster.create_index(create_timestamp=rounded_timestamp, settings=self.settings)
                    except Exception, e:
                        if "IndexAlreadyExistsException" not in e:
                            Log.error("Problem creating index", cause=e)
                    es.add_alias(self.settings.index)
            else:
                es = elasticsearch.Index(read_only=False, alias=best.alias, index=best.index, settings=self.settings)
            try:
                es.set_refresh_interval(seconds=60 * 10, timeout=10)
            except Exception, e:
                Log.warning("could not set refresh internal", cause=e)
            queue = self.known_queues[rounded_timestamp.unix] = es.threaded_queue(max_size=self.settings.queue_size, batch_size=self.settings.batch_size, silent=True)
        return queue

    # ADD keys() SO ETL LOOP CAN FIND WHAT'S GETTING REPLACED
    def keys(self, prefix=None):
        path = jx.reverse(etl2path(key2etl(prefix)))

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

    def extend(self, documents, queue=None):
        if queue == None:
            doc = documents[0]
            if doc.value:
                queue = self._get_queue(doc.value)
            elif doc.json:
                queue = self._get_queue(convert.json2value(doc.json))

        queue.extend(documents)

    def add(self, doc, queue=None):
        if queue == None:
            if doc.value:
                queue = self._get_queue(doc.value)
            elif doc.json:
                queue = self._get_queue(convert.json2value(doc.json))

        queue.add(doc)

    def delete(self, filter):
        self.es.delete(filter)

    def copy(self, keys, source, sample_only_filter=None, sample_size=None, done_copy=None):
        """
        :param keys: THE KEYS TO LOAD FROM source
        :param source: THE SOURCE (USUALLY S3 BUCKET)
        :param sample_only_filter: SOME FILTER, IN CASE YOU DO NOT WANT TO SEND EVERYTHING
        :param sample_size: FOR RANDOM SAMPLE OF THE source DATA
        :param done_copy: CALLBACK, ADDED TO queue, TO FINISH THE TRANSACTION
        :return: LIST OF SUB-keys PUSHED INTO ES
        """
        num_keys = 0
        queue = None
        for key in keys:
            try:
                for rownum, line in enumerate(source.read_lines(strip_extension(key))):
                    if not line:
                        continue

                    row, please_stop = fix(rownum, line, source, sample_only_filter, sample_size)
                    num_keys += 1

                    if queue == None:
                        queue = self._get_queue(row)
                    queue.add(row)

                    if please_stop:
                        break
            except Exception, e:
                done_copy = None
                Log.warning("Could not process {{key}}", key=key, cause=e)

        if done_copy:
            if queue == None:
                done_copy()
            else:
                queue.add(done_copy)

        return num_keys


def fix(rownum, line, source, sample_only_filter, sample_size):
    # ES SCHEMA IS STRICTLY TYPED, USE "code" FOR TEXT IDS
    line = line.replace('{"id": "bb"}', '{"code": "bb"}').replace('{"id": "tc"}', '{"code": "tc"}')

    # ES SCHEMA IS STRICTLY TYPED, THE SUITE OBJECT CAN NOT BE HANDLED
    if source.name.startswith("active-data-test-result"):
        # "suite": {"flavor": "plain-chunked", "name": "mochitest"}
        found = strings.between(line, '"suite": {', '}')
        if found:
            suite_json = '{' + found + "}"
            if suite_json:
                suite = convert.json2value(suite_json)
                suite = convert.value2json(suite.name)
                line = line.replace(suite_json, suite)

    if rownum == 0:
        value = convert.json2value(line)
        if len(line) > 100000:
            value.result.subtests = [s for s in value.result.subtests if s.ok is False]
            value.result.missing_subtests = True

        _id, value = _fix(value)
        row = {"id": _id, "value": value}
        if sample_only_filter and Random.int(int(1.0/coalesce(sample_size, 0.01))) != 0 and jx.filter([value], sample_only_filter):
            # INDEX etl.id==0, BUT NO MORE
            if value.etl.id != 0:
                Log.error("Expecting etl.id==0")
            return row, True
    elif len(line) > 100000:
        value = convert.json2value(line)
        value.result.subtests = [s for s in value.result.subtests if s.ok is False]
        value.result.missing_subtests = True
        _id, value = _fix(value)
        row = {"id": _id, "value": value}
    elif line.find("\"resource_usage\":") != -1:
        value = convert.json2value(line)
        _id, value = _fix(value)
        row = {"id": _id, "value": value}
    else:
        # FAST
        _id = strings.between(line, "\"_id\": \"", "\"")  # AVOID DECODING JSON
        row = {"id": _id, "json": line}

    return row, False


def _fix(value):
    if value.repo._source:
        value.repo = value.repo._source
    if not value.build.revision12:
        value.build.revision12 = value.build.revision[0:12]
    if value.resource_usage:
        value.resource_usage = None

    _id = value._id

    return _id, value
