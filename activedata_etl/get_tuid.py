# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from mo_logs import Log, startup, constants
from tuid.client import TuidClient


try:
    settings = startup.read_settings()
    constants.set(settings.constants)
    Log.start(settings.debug)
    client = TuidClient(settings.tuid_client)

    tuids = client.get_tuid(revision="29dcc9cb77c372c97681a47496488ec6c623915d", file="gfx/thebes/gfxFontVariations.h")
    Log.note("tuids = {{tuids}}", tuids=tuids)
except Exception as e:
    Log.error("Problem with tuid client test", e)
finally:
    Log.stop()

