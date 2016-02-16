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

DEBUG = True

def process(source_key, source, destination, resources, please_stop=None):
    with open(source) as json_file:
        json_data = wrap(json.load(json_file))
        output_lines = []
        for obj in json_data:
            for line in obj.covered:
                new_line = {
                    "test": {
                        "testUrl": obj.testUrl
                    },
                    "source": {
                        "sourceFile": obj.sourceFile,
                        "lineCovered": line
                    }
                }
                output_lines.append(new_line)

    return output_lines
