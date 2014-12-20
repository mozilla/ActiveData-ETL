# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.env.elasticsearch import Cluster
from pyLibrary.env.pulse import Pulse
from pyLibrary.thread.threads import Thread


def got_result(es, message):
    Log.note("{{message}}", locals())


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            es = Cluster(settings.destination).get_or_create_index(settings.destination)
            with Pulse(settings.source, got_result, es=es):
                Thread.sleep()

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
