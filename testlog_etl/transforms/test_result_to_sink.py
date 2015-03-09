# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division

from pyLibrary import convert
from pyLibrary.debugs.logs import Log


def process_test_result(source_key, source, destination, please_stop=None):

    try:
        lines = source.read_lines()
    except Exception, e:
        if "does not exist" in e:
            return set()
        else:
            Log.error("Problem reading lines", e)

    keys=[]
    data = []
    for l in lines:
        record = convert.json2value(l)
        if record._id==None:
            continue
        keys.append(record._id)
        data.append({
            "id": record._id,
            "value": record
        })
        record._id = None
    if data:
        destination.extend(data)
    return set(keys)
