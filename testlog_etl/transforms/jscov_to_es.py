# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Trung Do (chin.bimbo@gmail.com)
#
from __future__ import division
from __future__ import unicode_literals

import json

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict
from pyLibrary.dot import wrap
from pyLibrary.env import http
from testlog_etl.transforms import EtlHeadGenerator


def process(source_key, source, destination, resources, please_stop=None):
    """
    This transform will turn a pulse message containing info about a jscov artifact on taskcluster
    into a list of records of line coverages. Each record represents a line.
    :param source_key: The key of the file containing the pulse messages in the source pulse message bucket
    :param source: The source pulse messages, in a batch of (usually) 100
    :param destination: The destination for the transformed data
    :param resources: not used
    :param please_stop: The stop signal to stop the current thread
    :return: The list of keys of files in the destination bucket
    """
    keys = []
    records = []
    etl_header_gen = EtlHeadGenerator(source_key)
    print "Processing " + source_key
    bucket_file_count = -1

    for msg_line_index, msg_line in enumerate(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        pulse_record = convert.json2value(msg_line)
        artifact_file_name = pulse_record.artifact.name

        # we're only interested in jscov files, at lease at the moment
        if "jscov" not in artifact_file_name:
            continue

        # create the key for the file in the bucket, and add it to a list to return later
        bucket_file_count += 1
        bucket_key = source_key + "." + unicode(bucket_file_count)
        keys.append(bucket_key)

        # construct the artifact's full url
        taskId = pulse_record.status.taskId
        runId = pulse_record.runId
        full_artifact_path = "https://public-artifacts.taskcluster.net/" + taskId + "/" + unicode(runId) + "/" + artifact_file_name

        # fetch the artifact
        response = http.get(full_artifact_path).all_content

        # transform
        json_data = wrap(json.loads(response))
        for source_file_index, obj in enumerate(json_data):
            if please_stop:
                Log.error("Shutdown detected. Stopping job ETL.")

            # get the test name. Just use the test file name at the moment
            # TODO: change this when needed
            test_name = obj.testUrl.split("/")[-1]

            for line_index, line in enumerate(obj.covered):
                _, dest_etl = etl_header_gen.next(pulse_record.etl, source_file_index)

                # reusing dest_etl.id, which should be continuous
                record_key = bucket_key + "." + unicode(dest_etl.id)

                new_line = wrap({
                    "test": {
                        "name": test_name,
                        "url": obj.testUrl
                    },
                    "source": {
                        "file": obj.sourceFile,
                        "covered": line
                    },
                    "etl": dest_etl
                })

                # file marker
                if line_index == 0:
                    new_line.source.is_file = "true"

                records.append({"id": record_key, "value": new_line})

    destination.extend(records)
    return keys
