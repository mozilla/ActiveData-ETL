# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

# NEED TO BE NOTIFIED OF ID TO REPROCESS
# NEED TO BE NOTIFIED OF RANGE TO REPROCESS
# MUST SEND CONSEQUENCE DOWN THE STREAM SO OTHERS CAN WORK ON IT
from boto.sqs.connection import SQSConnection
from pyLibrary.queries import Q

from testlog_etl.pulse_block_to_unittest_logs import process_pulse_block
from testlog_etl.pulse_block_to_talos_logs import process_talos

from pyLibrary import convert
from pyLibrary import aws
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.structs import wrap, Struct

# a = {
# "from": "pulse_s3",
#     "target": convert,
#     "where": {"and": [
#         {"term": {"etl.name": "s3"}},
#         {"range": {"etl.id": {"gte": "1000"}}}
#     ]}
# }



# source bucket
# destination bucket
# transformer
# step in the whole process (for determining name pattern)
# overall agg or join

workers = [
    {
        "name": "pulse2unittest",
        "source": "all-pulse-testing",
        "destination": "all-unittest-testing",
        "transformed": process_pulse_block,
        "type": "join"
    },
    {
        "name": "pulse2talos",
        "source": "all-pulse-testing",
        "destination": "all-talos-testing",
        "transformed": process_talos,
        "type": "join"
    }
]


def pipe(work_queue, connection):
    """
    :param work_queue:
    :param connection:
    :return: False IF THERE IS NOTHING LEFT TO DO
    """
    message = work_queue.recieve_message()
    if message == None:
        return False

    source_block = convert.json2value(message.get_body())
    if source_block.key:
        source_keys = [source_block.key]
    else:
        source_keys = source_block.keys

    w = wrap([w for w in workers if w.source == source_block.bucket]).first()
    if w == None:
        Log.error("Could not process records from {{bucket}}", {"bucket": source_block.bucket})

    try:
        source = ConcatSources([connection.get_bucket(source_block.bucket).get_key(k) for k in source_keys])
        dest_bucket = connection.get_bucket(w.destination.bucket)
        old_keys = dest_bucket.keys(prefix=source_block.key)
        new_keys = w.transformer(source, dest_bucket)

        for k in old_keys - new_keys:
            dest_bucket.delete_key(k)

        for k in old_keys + new_keys:
            work_queue.send_message(convert.value2json({
                "bucket": w.destination.bucket,
                "key": k
            }))
        work_queue.delete_message(message)
    except Exception, e:
        Log.error("Problem transforming", e)


class ConcatSources(object):
    """
    MAKE MANY SOURCES LOOK LIKE ONE
    """

    def __init__(self, sources):
        self.source = sources

    def read(self):
        return "\n".join(s.read() for s in self.sources)


def query_to_work(query, work_queue, connection):
    """
    CONVERT A QUERY TO A LIST OF WORK ITEMS, WHICH IS INSERTED INTO THE QUEUE
    """

    # DETERMINE WHAT STAGE IS AFFECTED
    for w in workers:
        if w.source != query["from"]:
            continue

        #FIND IDS THAT MUST BE REPROCESSED
        source_keys = connection.get_bucket(w.source).keys()
        todo = Q.run({
            "from": [key2struct(id) for id in source_keys],
            "select": "id",
            "where": query.where
        })

        for k in source_keys:
            destination_keys = connection.get_bucket(w.destination).keys(pcre="")


        S3Key.get_contents_to_file(tempfile, headers={'Range': 'bytes=0-100000'}



        # DETERMINE WORK ITEMS
        # GO UP THE ETL PATH TO SEE IF AGGREGATION IS REQUIRED
        # GO BACK DOWN THE PATH, CLUSTERING THE SOURCES (AND ADDING MORE WORK ITEMS)



def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        work_queue = SQSConnection(settings.aws).get_queue("ekyle-unittest-testing")
        with aws.s3.Connection(settings.aws) as connection:
            keep_going = True
            while keep_going:
                try:
                    keep_going = pipe(work_queue, connection)
                except Exception, e:
                    Log.warning("Can not handle work", e)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


