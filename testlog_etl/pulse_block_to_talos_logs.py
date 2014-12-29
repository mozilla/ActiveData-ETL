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

from pyLibrary import convert
from pyLibrary.debugs.logs import Log


def process_talos(source, worker):
    """
    SIMPLE CONVERT pulse_block INTO S3 LOGFILES
    """
    for line in source.read().split("\n"):
        envelope = convert.json2value(line)
        if envelope.data.talos:
            try:
                zlib.compress(string, level=9)

                log_content = requests.get(url)
                dest_key = envelope.data.etl.id + ":" + envelope.data.etl.source_id
                dest_bucket.write(dest_key, envelope+"\n"+log_content)
            except Exception, e:
                Log.error("Problem processing {{url}}", {"url": url}, e)
