# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# MIMICS THE requests API (http://docs.python-requests.org/en/latest/)
# WITH ADDED default_headers THAT CAN BE SET USING pyLibrary.debugs.settings
# EG
# {"debug.constants":{
# "pyLibrary.env.http.default_headers={
# "From":"klahnakoski@mozilla.com"
#     }
# }}


from __future__ import unicode_literals
from __future__ import division
import os
from tempfile import TemporaryFile
from pyLibrary.debugs.logs import Log


MIN_READ_SIZE = 8 * 1024


class FileString(object):
    """
    ACTS LIKE A STRING, BUT IS A FILE
    """

    def __init__(self, blocks, raw=None):
        if hasattr(blocks, "read"):
            self.file = blocks
            return

        self.file = TemporaryFile()
        for b in blocks:
            self.file.write(b)
        while raw._fp.fp is not None:
            self.file.write(raw.read(amt=MIN_READ_SIZE, decode_content=True))


    def decode(self, encoding):
        if encoding != "utf8":
            Log.error("can not handle {{encoding}}", {"encoding": encoding})
        self.encoding = encoding
        return self

    def split(self, sep):
        if sep != "\n":
            Log.error("Can only split by lines")
        self.file.seek(0)
        return (l.decode(self.encoding) for l in self.file)

    def __len__(self):
        return os.path.getsize(self.file.name)

    def __add__(self, other):
        self.file.seek(0, 2)
        self.file.write(other)

    def __radd__(self, other):
        new_file = TemporaryFile()
        new_file.write(other)
        self.file.seek(0)
        for l in self.file:
            new_file.write(l)
        new_file.seek(0)
        return FileString(new_file)

    def __getattr__(self, attr):
        return getattr(self.file, attr)

    def __del__(self):
        self.file.close()

    def __iter__(self):
        self.file.seek(0)
        return self.file
