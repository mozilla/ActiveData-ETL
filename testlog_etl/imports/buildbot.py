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


BUILDBOT_LOGS = "http://builddata.pub.build.mozilla.org/builddata/buildjson/"


STATUS_CODES = {
    0: "success",
    1: "warnings",
    2: "failure",
    3: "skipped",
    4: "exception",
    5: "retry",
    6: "cancelled",
    "0": "success",
    "1": "warnings",
    "2": "failure",
    "3": "skipped",
    "4": "exception",
    "5": "retry",
    "6": "cancelled",
    None: None,
    "success (0)": "success",
    "warnings (1)": "warnings",
    "failure (2)": "failure",
    "skipped (3)": "skipped",
    "exception (4)": "exception",
    "retry (5)": "retry",
    "cancelled (6)": "cancelled"
}
