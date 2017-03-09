# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from math import log10

from mo_kwargs import override

from activedata_etl import etl2key, key2etl
from activedata_etl.reset import Version
from mo_collections import UniqueIndex
from mo_dots import wrap
from mo_logs import Log
from mo_math import Math
from mo_times.timer import Timer
from pyLibrary import convert
from pyLibrary.aws import s3
from pyLibrary.aws.s3 import key_prefix


class S3Bucket(object):

    @override
    def __init__(
        self,
        bucket,  # NAME OF THE BUCKET
        aws_access_key_id=None,  # CREDENTIAL
        aws_secret_access_key=None,  # CREDENTIAL
        region=None,  # NAME OF AWS REGION, REQUIRED FOR SOME BUCKETS
        public=False,
        debug=False,
        kwargs=None
    ):
        self.bucket = s3.Bucket(kwargs)
        self.settings = kwargs

    def __getattr__(self, item):
        return getattr(self.bucket, item)

    def keys(self, prefix):
        metas = self.bucket.metas(prefix=prefix)
        return set(metas.key)

    def find_keys(self, start, count, filter=None):
        digits = int(Math.ceiling(log10(count - 1)))
        prefix = unicode(start)[:-digits]

        metas = self.bucket.metas(prefix=prefix)
        min_ = Version(unicode(start))
        max_ = Version(unicode(start+count))
        output = [m.key for m in metas if min_ <= Version(m.key) < max_]

        return set(output)

    def find_largest_key(self):
        """
        FIND LARGEST VERSION NUMBER (with dots (.) and colons(:)) IN
        THE KEYS OF AN S3 BUCKET.
        """
        with Timer("Full scan of {{bucket}} for max key", {"bucket": self.bucket.name}):
            maxi = 0
            for k in self.bucket.bucket.list(delimiter=":"):
                try:
                    v = key_prefix(k.name)
                    maxi = max(maxi, v)
                except Exception as e:
                    self.bucket.bucket.delete_key(k.name)
            return maxi

    def extend(self, documents, overwrite=False):
        parts = {}
        for d in wrap(documents):
            parent_key = etl2key(key2etl(d.id).source)
            d.value._id = d.id
            sub = parts.setdefault(parent_key, [])
            sub.append(d.value)

        for k, docs in parts.items():
            self._extend(k, docs, overwrite=overwrite)

        return set(parts.keys())

    def _extend(self, key, documents, overwrite=False):
        if overwrite:
            self.bucket.write_lines(key, (convert.value2json(d) for d in documents))
            return

        meta = self.bucket.get_meta(key)
        if meta != None:
            documents = UniqueIndex(keys="etl.id", data=documents)
            try:
                content = self.bucket.read_lines(key)
                old_docs = UniqueIndex(keys="etl.id", data=map(convert.json2value, content))
            except Exception as e:
                Log.warning("problem looking at existing records", e)
                # OLD FORMAT (etl header, followed by list of records)
                old_docs = UniqueIndex(keys="etl.id")

            residual = old_docs - documents
            overlap = old_docs & documents
            # IS IT CHEAPER TO SEE IF THERE IS A DIFF, RATHER THAN WRITE NEW DATA TO S3?

            # CAN NOT PERFORM FUZZY MATCH, THE etl PROPERTY WILL HAVE CHANGED
            # fuzzytestcase.assertAlmostEqual(documents._data, overlap._data)

            if residual:
                documents = documents | residual

        self.bucket.write_lines(key, (convert.value2json(d) for d in documents))

    def add(self, dco):
        Log.error("Not supported")

