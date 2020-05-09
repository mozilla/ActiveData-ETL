# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import division
from __future__ import unicode_literals

from mo_logs import Log
from mo_logs import startup
from mo_times.durations import SECOND
from pyLibrary.aws import sqs


def copy_queue(settings):
    source = sqs.Queue(settings.source)
    destination = sqs.Queue(settings.destination)

    while True:
        m = source.pop(wait=10*SECOND)
        if not m:
            break
        destination.add(m)
    source.commit()


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        copy_queue(settings)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
