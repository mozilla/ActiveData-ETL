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

from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import Dict
from pyLibrary.env import http
from pyLibrary.thread.threads import Signal
from testlog_etl.transforms import EtlHeadGenerator, verify_blobber_file
from testlog_etl.transforms.unittest_logs_to_sink import process_unittest

DEBUG = False
DEBUG_SHOW_LINE = True
DEBUG_SHOW_NO_LOG = False
PARSE_TRY = True

SINGLE_URL = None


def process(source_key, source, destination, resources, please_stop=None):
    """
    READ pulse_block AND THE REFERENCED STRUCTURED LOG FILES
    TRANSFORM STRUCTURED LOG TO INDIVIDUAL TESTS
    """
    output = []

    existing_keys = destination.keys(prefix=source_key)
    for e in existing_keys:
        destination.delete_key(e)

    for i, line in enumerate(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping early")

        tc = convert.json2value(line)

        # REVIEW THE ARTIFACTS, LOOK FOR STRUCTURED LOGS
        for j, a in enumerate(tc.artifacts):
            lines, num_bytes = verify_blobber_file(j, a.name, a.url)
            if lines:
                process_unittest(source_key, tc.etl, tc, lines, destination, please_stop=please_stop)

    return output


