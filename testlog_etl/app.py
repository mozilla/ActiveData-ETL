# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
import functools

import requests

from pyLibrary import convert
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.env.pulse import Pulse
from pyLibrary.maths import Math
from pyLibrary.queries import Q
from pyLibrary.structs import wrap, set_default, Dict, nvl
from pyLibrary.thread.threads import Thread, Queue
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer





def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            es = Cluster(settings.destination).get_or_create_index(settings.destination)
            pulse = Pulse(settings.source, target=functools.partial(process_unittest, sink=es))

            Thread.wait_for_shutdown_signal()
            pulse.stop()
            pulse.join()

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
