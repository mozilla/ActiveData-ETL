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

from pyLibrary.dot import wrap
from pyLibrary.env.git import get_git_revision

DEBUG = True

def process(source_key, source, destination, resources, please_stop=None):
    with open(source) as json_file:
        json_data = wrap(json.load(json_file))
        output_lines = []
        for i, obj in enumerate(json_data):
            # get the test name. Just use the test file name at the moment
            # TODO: change this when needed
            last_slash_index = obj.testUrl.rfind("/")
            test_name = obj.testUrl[last_slash_index + 1:]

            for line in obj.covered:
                new_line = {
                    "test": {
                        "name": test_name,
                        "url": obj.testUrl
                    },
                    "source": {
                        "sourceFile": obj.sourceFile,
                        "lineCovered": line
                    },
                    "etl": {
                        "type": "join",
                        "revision": get_git_revision()
                    }
                }
                output_lines.append(new_line)

    return output_lines
