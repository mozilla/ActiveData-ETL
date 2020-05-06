# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import division
from __future__ import unicode_literals

import hashlib

import mo_json_config
from jx_elasticsearch import elasticsearch
from mo_files import File
from mo_logs import Log
from mo_threads import THREAD_STOP

es_config = mo_json_config.get("file://resources/settings/codecoverage/push_cv_to_es.json").elasticsearch

es_config.host = "http://activedata.allizom.org"

es = elasticsearch.Cluster(es_config).get_or_create_index(es_config)
queue = es.threaded_queue(batch_size=100)

dir_ = File("../CoverageUtils-Trung/ActivData/transformed")
for f in dir_.children:
    Log.note("Adding data from {{file}}", file=f.abspath)
    for line in f.read_bytes().decode('utf8').splitlines():
        queue.add({"id": hashlib.md5(line).hexdigest(), "json": line})

queue.add(THREAD_STOP)
