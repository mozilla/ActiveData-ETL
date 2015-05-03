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
from pyLibrary.aws.s3 import key_prefix
from pyLibrary.debugs.logs import Log
from pyLibrary.thread.threads import Lock

INDEX_TRY = False

is_done_lock = Lock()
is_done = set()

def process_test_result(source_key, source, destination, please_stop=None):
    lines = source.read_lines()

    keys=[]
    data = []
    for l in lines:
        record = convert.json2value(l)
        if record._id==None:
            continue
        if not INDEX_TRY:
            if record.build.branch == "try":
                return {}
        keys.append(record._id)
        data.append({
            "id": record._id,
            "value": record
        })
        record._id = None
    if data:
        try:
            destination.extend(data)
        except Exception, e:
            if "Can not decide on index by build.date" in e:
                if source.bucket.name == "ekyle-test-result":
                    # KNOWN CORRUPTION
                    # TODO: REMOVE LATER (today = Mar2015)
                    delete_list = source.bucket.keys(prefix=key_prefix(source_key))
                    for d in delete_list:
                        source.bucket.delete_key(d)
            Log.error("Can not add to sink", e)

    return set(keys)
