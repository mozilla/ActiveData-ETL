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
from pyLibrary.env import http
from testlog_etl.transforms import EtlHeadGenerator
from testlog_etl.transforms.pulse_block_to_es import scrub_pulse_record


def process(source_key, source, destination, resources, please_stop=None):
    keys = []
    records = []
    etl_header_gen = EtlHeadGenerator(source_key)

    for i, line in enumerate(source.read_lines()):
        stats = Dict()
        pulse_record = scrub_pulse_record(source_key, i, line, stats)
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
            # get the test name. Just use the test file name at the moment
            # TODO: change this when needed
            test_name = obj.testUrl.split("/")[-1]

            for line in obj.covered:
                dest_key, dest_etl = etl_header_gen.next(pulse_record.etl, j)
                key = dest_key + "." + unicode(j)
                new_line = {
                    "test": {
                        "name": test_name,
                        "url": obj.testUrl
                    },
                    "source": {
                        "sourceFile": obj.sourceFile,
                        "lineCovered": line
                    },
                    "etl": dest_etl
                }
                records.append({"id": key, "value": new_line})
                keys.append(key)

    destination.extend(records)
    return keys
