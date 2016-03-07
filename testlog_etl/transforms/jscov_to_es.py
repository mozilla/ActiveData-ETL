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

from pyLibrary.dot import Dict
from pyLibrary.dot import wrap
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from testlog_etl.transforms import EtlHeadGenerator
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record


def process(source_key, source, destination, resources, please_stop=None):
    keys = []
    records = []
    etl_header_gen = EtlHeadGenerator(source_key)
    print "Processing " + source_key

    for i, msg_line in enumerate(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        bucket_key = source_key + "." + str(i)
        stats = Dict()
        pulse_record = scrub_pulse_record(source_key, i, msg_line, stats)
        artifact_file_name = pulse_record.artifact.name

        # we're only interested in jscov files, at lease at the moment
        if "jscov" not in artifact_file_name:
            continue

        # construct the artifact's full url
        taskId = pulse_record.status.taskId
        runId = pulse_record.runId
        full_artifact_path = "https://public-artifacts.taskcluster.net/" + taskId + "/" + str(runId) + "/" + artifact_file_name

        # fetch the artifact
        response = http.get(full_artifact_path).all_content

        # transform
        json_data = wrap(json.loads(response))
        for j, obj in enumerate(json_data):
            if please_stop:
                Log.error("Shutdown detected. Stopping job ETL.")

            # get the test name. Just use the test file name at the moment
            # TODO: change this when needed
            test_name = obj.testUrl.split("/")[-1]

            for line in obj.covered:
                _, dest_etl = etl_header_gen.next(pulse_record.etl, j)

                # reusing dest_etl.id, which should be continuous
                record_key = bucket_key + "." + str(dest_etl.id)

                new_line = {
                    "test": {
                        "name": test_name,
                        "url": obj.testUrl
                    },
                    "source": {
                        "file": obj.sourceFile,
                        "covered": line
                    },
                    "etl": dest_etl
                }
                records.append({"id": record_key, "value": new_line})
                keys.append(record_key)

    destination.extend(records)
    return keys
