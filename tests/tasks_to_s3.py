# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from mo_times import Date
from pyLibrary.queries import jx

import mo_json_config
from mo_logs import startup, constants, Log
from pyLibrary import convert
from pyLibrary.aws import s3, Queue

from mo_files import File

START, STOP = 560253, 560325
NUM = STOP - START


def work(settings):
    data = File("~/documents/taskids").read()
    all_messages = {}
    for i in map(lambda x: x.strip(), data.split("\n")):
        all_messages[i] = {"status": {"taskId": i}}
    data = File("~/documents/taskids2.txt").read()
    for i in map(lambda x: x.strip(), data.split("\n")):
        all_messages[i] = {"status": {"taskId": i}}

    # GET CONTENT OF S3 RANGE
    bucket = s3.Bucket(settings.bucket)
    for id in range(START, STOP, 1):
        s3_file = bucket.get_key("tc." + unicode(id))
        local_file = File("~/" + unicode(id) + ".json")
        # local_file.write(s3_file.read())
        for line in local_file.read_lines():
            m = convert.json2value(line)
            id = m.status.taskId
            if id not in all_messages:
                Log.note("net new task {{id}}", id=id)
            all_messages[id] = m

    # REWRITE THE BUCKETS WITH CONTENT
    done = Queue(settings.queue)
    num_messages = len(all_messages)
    for i, messages in jx.groupby(all_messages.values(), size=7000):
        id = START + i
        if id >= STOP:
            Log.error("not expected")
        key = "tc." + unicode(id)
        file = bucket.get_key(key)
        file.write_lines(map(convert.value2json, messages))
        done.add({
            "bucket": "active-data-task-cluster-logger",
            "date/time": Date.now().format(),
            "key": key,
            "timestamp": Date.now()
        })


def main():

    try:
        settings = mo_json_config.expand(
            {
                "bucket": {
                    "bucket": "active-data-task-cluster-logger",
                    "public": True,
                    "key_format": "t.a:b",
                    "$ref": "file://~/private.json#aws_credentials"
                },
                "queue": {
               		"name":"active-data-etl",
               		"$ref": "file://~/private.json#aws_credentials"
               	}
            },
            "file://settings.json"
        )
        Log.start()
        work(settings)
    except Exception, e:
        Log.error("Problem with getting old data", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

