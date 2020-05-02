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

from mo_future import text
from mo_json import json2value, value2json
from pyLibrary.aws.s3 import Connection
from mo_logs import startup
from mo_logs import Log
from jx_python import jx


def main():
    """
    CLEAR OUT KEYS FROM BUCKET BY RANGE, OR BY FILE
    """
    settings = startup.read_settings(defs=[
        {
            "name": ["--bucket"],
            "help": "bucket to scan",
            "type": str,
            "dest": "bucket",
            "required": True
        }
    ])
    Log.start(settings.debug)

    source = Connection(settings.aws).get_bucket(settings.args.bucket)

    for k in jx.sort(source.keys()):
        try:
            data = source.read_bytes(k)
            if convert.ascii2unicode(data).find("2e2834fa7ecd8d3bb1ad49ec981fdb89eb4df95e18") >= 0:
                Log.note("Found at {{key}}", key=k)
        except Exception as e:
            Log.warning("Problem with {{key}}", key=k, cause=e)
        finally:
            Log.stop()


if __name__ == "__main__":
    main()
