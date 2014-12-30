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
from distutils.command.clean import clean

import boto
from pyLibrary import convert
from pyLibrary.aws import cleanup

from pyLibrary.debugs.logs import Log
from pyLibrary.structs import nvl, Null


READ_ERROR="S3 read error"



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
        bucket - NAME OF THE BUCKET
        aws_access_key_id - CREDENTIAL
        aws_secret_access_key - CREDENTIAL
        """
        self.settings = settings

        try:
            cleanup(self.settings)
            self.connection = boto.connect_s3(
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
        bucket - NAME OF THE BUCKET
        aws_access_key_id - CREDENTIAL
        aws_secret_access_key - CREDENTIAL
        """
        self.settings = settings
        self.settings.public = nvl(self.settings.public, public)
        self.connection = None
        self.bucket = None

        if settings==None:
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
        return set(k.key for k in self.bucket.list(prefix=prefix))

    def read(self, key):
        try:
            value = self.bucket.get_key(key)
        except Exception, e:
            Log.error(READ_ERROR, e)

        if value == None:
            return None

        try:
            json = value.get_contents_as_string()
        except Exception, e:
            Log.error(READ_ERROR, e)

        if json == None:
            return None
        return convert.utf82unicode(json)



    def write(self, key, value):
        try:
            key = self.bucket.new_key(key)
            if isinstance(value, str):
                key.set_contents_from_string(value)
            else:
                key.set_contents_from_string(convert.unicode2utf8(value))
            if self.settings.public:
                key.set_acl('public-read')
        except Exception, e:
            Log.error("S3 write error", e)


    @property
    def name(self):
        return self.settings.bucket
