# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

import requests
import zlib

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.structs import wrap
from testlog_etl.rpq import etl2key


def process_pulse_block(source, dest_bucket):
    """
    SIMPLE CONVERT pulse_block INTO S3 LOGFILES
    """
    for line in source.read().split("\n"):
        envelope = convert.json2value(line)
        file_num = 0
        for name, url in envelope.data.blobber_files.items():
            try:
                if "structured" in name and name.endswith(".log"):
                    log_content = requests.get(url)
                    dest_etl = wrap({"id": file_num, "name": name, "source": envelope.data.etl, "type": "join"})
                    dest_key = etl2key(dest_etl)
                    dest_bucket.write(dest_key, zlib.compress(dest_etl+"\n"+log_content, 9))
                    file_num+=1
            except Exception, e:
                Log.error("Problem processing {{url}}", {"url": url}, e)

        if not file_num:
            dest_key = "(" + envelope.data.etl.id + ":" + envelope.data.etl.source_id + ").0"
            dest_bucket.write(dest_key, envelope)

