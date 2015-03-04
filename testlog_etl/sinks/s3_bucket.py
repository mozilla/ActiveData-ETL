# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary import convert
from pyLibrary.aws import s3

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, Dict, literal_field
from pyLibrary.meta import use_settings
from pyLibrary.queries.unique_index import UniqueIndex
from testlog_etl import etl2key, key2etl


class S3Bucket(object):

    @use_settings
    def __init__(
        self,
        bucket,  # NAME OF THE BUCKET
        aws_access_key_id,  # CREDENTIAL
        aws_secret_access_key,  # CREDENTIAL
        region=None,  # NAME OF AWS REGION, REQUIRED FOR SOME BUCKETS
        public=False,
        debug=False,
        settings=None
    ):
        self.bucket = s3.Bucket(settings)
        self.settings = settings

    def __getattr__(self, item):
        return getattr(self.bucket, item)

    def keys(self, prefix):
        metas = self.bucket.metas(prefix=prefix)
        output = []
        for m in metas:
            for line in self.bucket.read_lines(m.key):
                try:
                    id = etl2key(convert.json2value(line).etl)
                    output.append(id)
                except Exception, _:
                    pass
        return set(output)

    def extend(self, documents):
        parts = Dict()
        for d in wrap(documents):
            parent_key = literal_field(etl2key(key2etl(d.id).source))
            parts[parent_key] += [d.value]

        for k, docs in parts.items():
            self._extend(k, docs)


    def _extend(self, key, documents):
        meta = self.bucket.get_meta(key)
        if meta is not None:
            documents = UniqueIndex(keys="etl.id", data=documents)
            old_docs = UniqueIndex(keys="etl.id", data=map(convert.json2value, self.bucket.read_lines(key)))
            documents = documents | (old_docs - documents)

        self.bucket.write_lines(key, map(convert.value2json, documents))

    def add(self, dco):
        Log.error("Not supported")
