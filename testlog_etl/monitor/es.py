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

import requests
from fabric.operations import local

from pyLibrary import convert
from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log

try:
    # ATTEMPT TO HIDE WARNING SO *.error.log DOES NOT FILL UP
    from Crypto.pct_warnings import PowmInsecureWarning
    import warnings
    warnings.simplefilter("ignore", PowmInsecureWarning)
except Exception:
    pass


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        Log.note("Search ES...")
        result = requests.post(
            "http://localhost:9200/unittest/_search",
            data='{\"fields\":[\"etl.id\"],\"query\": {\"match_all\": {}},\"from\": 0,\"size\": 1}'
        )
        data = convert.json2value(convert.utf82unicode(result.content))

        if data._shards.failed > 0 or result.status_code != 200:
            # BAD RESPONSE, ASK SUPERVISOR FOR A RESTART
            Log.warning("ES gave a bad response. NO ACTION TAKEN.\n{{response|json|indent}}", response=data)
        else:
            Log.note("Good response")
    except Exception, e:
        Log.warning("Problem with call to ES.  NO ACTION TAKEN", cause=e)
    finally:
        Log.stop()

if __name__ == "__main__":
    main()
