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
import StringIO
import gzip
import zipfile

import boto
from boto.s3.connection import Location

from pyLibrary import convert
from pyLibrary.aws import cleanup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import nvl, Null


READ_ERROR = "S3 read error"


class File(object):
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    def read(self):
        return self.bucket.read(self.key)

    def write(self, value):
        self.bucket.write(self.key, value)


class Connection(object):
    def __init__(self, settings):
        """
        SETTINGS:
        region - NAME OF AWS REGION, REQUIRED FOR SOME BUCKETS
        bucket - NAME OF THE BUCKET
        aws_access_key_id - CREDENTIAL
        aws_secret_access_key - CREDENTIAL
        """
        self.settings = settings

        try:
            cleanup(self.settings)

            if not settings.region:
                self.connection = boto.connect_s3(
                    aws_access_key_id=self.settings.aws_access_key_id,
                    aws_secret_access_key=self.settings.aws_secret_access_key
                )
            else:
                self.connection = boto.s3.connect_to_region(
                    self.settings.region,
                    aws_access_key_id=self.settings.aws_access_key_id,
                    aws_secret_access_key=self.settings.aws_secret_access_key
                )
        except Exception, e:
            Log.error("Problem connecting to S3", e)


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()


    def get_bucket(self, name):
        output = Bucket(Null)
        output.bucket = self.connection.get_bucket(name, validate=False)
        return output


class Bucket(object):
    def __init__(self, settings, public=False):
        """
        SETTINGS:
        region - NAME OF AWS REGION, REQUIRED FOR SOME BUCKETS
        bucket - NAME OF THE BUCKET
        aws_access_key_id - CREDENTIAL
        aws_secret_access_key - CREDENTIAL
        """
        self.settings = settings
        self.settings.public = nvl(self.settings.public, public)
        self.connection = None
        self.bucket = None

        if settings == None:
            return

        try:
            self.connection = Connection(settings).connection
            self.bucket = self.connection.get_bucket(self.settings.bucket, validate=False)
        except Exception, e:
            Log.error("Problem connecting to {{bucket}}", {"bucket": self.settings.bucket}, e)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def get_key(self, key):
        return File(self, key)

    def keys(self, prefix=None):
        return set(strip_extension(k.key) for k in self.bucket.list(prefix=prefix))

    def read(self, key):
        if not isinstance(key, basestring):
            Log.error("Expecting key to be a string")

        if key.endswith(".json") or key.endswith(".zip") or key.endswith(".gz"):
            Log.error("Expecting a pure key")

        try:
            keys = list(self.bucket.list(prefix=key + ".json"))
            if len(keys) == 0:
                return None
            elif len(keys) > 1:
                Log.error("multiple keys with prefix={{prefix}}", {"prefix": key.key + ".json"})

            value = keys[0]
        except Exception, e:
            Log.error(READ_ERROR, e)

        try:
            json = value.get_contents_as_string()
        except Exception, e:
            Log.error(READ_ERROR, e)

        if json == None:
            return None

        if key.endswith(".zip"):
            json = _unzip(json)
        elif key.endswith(".gz"):
            json = _ungzip(json)

        return convert.utf82unicode(json)


    def write(self, key, value):
        if key.endswith(".json") or key.endswith(".zip"):
            Log.error("Expecting a pure key")

        try:
            if len(value) > 200 * 1000:
                if isinstance(value, str):
                    value = new_zipfile(key + ".json", value)
                    key += ".json.gz"
                else:
                    value = new_zipfile(key + ".json", convert.unicode2utf8(value))
                    key += ".json.gz"
            else:
                if isinstance(value, str):
                    key += ".json"
                else:
                    key += ".json"

            key = self.bucket.new_key(key)
            key.set_contents_from_string(value)

            if self.settings.public:
                key.set_acl('public-read')
        except Exception, e:
            Log.error("Problem writing {{bytes}} bytes to {{key}} in {{bucket}}", {
                "key": key.key,
                "bucket": self.bucket.name,
                "bytes": len(value)
            }, e)


    @property
    def name(self):
        return self.settings.bucket


def strip_extension(key):
    e = key.find(".json")
    if e == -1:
        return key
    return key[:e]


def new_zipfile(filename, content):
    buff = StringIO.StringIO()
    archive = gzip.GzipFile(fileobj=buff, mode='w')
    archive.write(content)
    archive.close()
    return buff.getvalue()


def _ungzip(compressed):
    buff = StringIO.StringIO(compressed)
    archive = gzip.open(buff, mode='r')
    return archive.read()

def _unzip(compressed):
    buff = StringIO.StringIO(compressed)
    archive = zipfile.ZipFile(buff, mode='r')
    return archive.read(archive.namelist()[0])
