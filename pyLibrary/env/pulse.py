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


    def __init__(self, settings, callback, **kwargs):
        Thread.__init__(self, name="Pulse consumer for " + settings.exchange, target=self._worker, **kwargs)
        self.callback = callback
        self.settings = set_default({}, settings, PulseConfiguration.defaults)
        self.pulse = GenericConsumer(settings, exchange = settings.exchange, connect=True, heartbeat=settings.heartbeat, **unwrap(settings))
        self.start()

    def _worker(self):
        while True:
            try:
                self.pulse.listen()
            except Exception, e:
                Log.warning("pulse had problem", e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pulse.disconnect()
        Thread.__exit__(self, exc_type, exc_val, exc_tb)
