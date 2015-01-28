# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import unicode_literals
from __future__ import division
from pyLibrary import convert, strings
from pyLibrary.aws.s3 import Bucket
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log


def fix(settings):
    bucket = Bucket(settings.stage)
    data = bucket.read("11890:1134372")

    data = data.replace("}{", "}\n{")
    # for i, line in enumerate(strings.split(data)):
    #     if i % 1000==0:
    #         Log.note("{{num}}", {"num": i})
    #     Log.note("{{json}}", {"json": convert.json2value(line)})

    bucket.write("11890:1134372", data)
    Log.alert("Done")


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        fix(settings)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
