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

from mo_future import text_type
import requests

from mo_json import json2value, value2json
from mo_logs import startup, constants
from mo_logs.exceptions import suppress_exception
from mo_logs import Log, machine_metadata

with suppress_exception:
    # ATTEMPT TO HIDE WARNING SO *.error.log DOES NOT FILL UP
    from Crypto.pct_warnings import PowmInsecureWarning
    import warnings
    warnings.simplefilter("ignore", PowmInsecureWarning)

def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        Log.note("Search ES...")
        result = requests.post(
            "http://localhost:9200/unittest/_search",
            data='{"fields":["etl.id"],"query": {"match_all": {}},"from": 0,"size": 1}'
        )
        data = json2value(convert.utf82unicode(result.content))

        if data._shards.failed > 0 or result.status_code != 200:
            # BAD RESPONSE, ASK SUPERVISOR FOR A RESTART
            Log.warning("ES gave a bad response. NO ACTION TAKEN.\n{{response|json|indent}}", response=data)
        else:
            Log.note("Good response")
    except Exception as e:
        Log.warning("Problem with call to ES at {{machine}}.  NO ACTION TAKEN", machine=machine_metadata, cause=e)
    finally:
        Log.stop()

if __name__ == "__main__":
    main()
