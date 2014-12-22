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

from mozillapulse.config import PulseConfiguration
from pyLibrary.debugs.logs import Log

from pyLibrary.structs import set_default, unwrap
from pyLibrary.thread.threads import Thread
from vendor.mozillapulse.consumers import GenericConsumer


class Pulse(Thread):

    def __init__(self, settings, callback):
        """
        settings IS A STRUCT WITH FOLLOWING PARAMETERS

            exchange - name of the Pulse exchange
            topic - message name pattern to subscribe to  ('#' is wildcard)

            host - url to connect (default 'pulse.mozilla.org'),
            port - tcp port (default ssl port 5671),
            user - (default 'public')
            password - (default 'public')
            vhost - http HOST (default '/'),
            ssl - True to use SSL (default True)

            applabel = unknown (default '')
            heartbeat - True to also get the Pulse heartbeat message (default False)
            durable - True to keep queue after shutdown (default (False)

            serializer - (default 'json')
            broker_timezone' - (default 'GMT')

        callback - function that accepts "message" - executed upon receiving message
        """
        Thread.__init__(self, name="Pulse consumer for " + settings.exchange, target=self._worker)
        self.settings = set_default({"broker_timezone": "GMT"}, settings, PulseConfiguration.defaults)
        self.settings.callback = callback
        self.pulse = GenericConsumer(self.settings, connect=True, **unwrap(self.settings))
        self.start()

    def _worker(self, please_stop):
        while not please_stop:
            try:
                self.pulse.listen()
            except Exception, e:
                Log.warning("pulse had problem", e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.please_stop.go()
        self.pulse.disconnect()
        Thread.__exit__(self, exc_type, exc_val, exc_tb)
