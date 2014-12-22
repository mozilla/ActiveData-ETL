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
from pyLibrary import convert
from pyLibrary.debugs.logs import Log

from pyLibrary.structs import set_default, unwrap, wrap, nvl
from pyLibrary.thread.threads import Thread, Queue
from vendor.mozillapulse.consumers import GenericConsumer


class Pulse(Thread):

    def __init__(self, settings, queue=None):
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

        if queue == None:
            self.queue = Queue()
        else:
            self.queue = queue

        Thread.__init__(self, name="Pulse consumer for " + settings.exchange, target=self._worker)
        self.settings = set_default({"broker_timezone": "GMT"}, settings, PulseConfiguration.defaults)
        self.settings.callback = self._got_result
        self.settings.user = nvl(self.settings.user, self.settings.username)

        self.pulse = GenericConsumer(self.settings, connect=True, **unwrap(self.settings))
        self.start()


    def _got_result(self, data, message):
        payload = wrap(data).payload
        if self.settings.debug:
            Log.note("{{data}}", {"data": payload})
        self.queue.add(convert.value2json(payload))
        message.ack()

    def _worker(self, please_stop):
        while not please_stop:
            try:
                self.pulse.listen()
            except Exception, e:
                if not please_stop:
                    Log.warning("pulse had problem", e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        Log.note("clean pulse exit")
        self.please_stop.go()
        self.queue.close()
        try:
            self.pulse.disconnect()
        except Exception, e:
            Log.warning("Can not disconnect during pulse exit, ignoring", e)
        Thread.__exit__(self, exc_type, exc_val, exc_tb)
