# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

from mo_future import text
from fabric.context_managers import cd
from fabric.operations import run, sudo
from fabric.state import env

from mo_logs import startup, constants
from mo_logs import Log


def _config_fabric(connect):
    for k, v in connect.items():
        env[k] = v
    env.abort_exception = Log.error


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        Log.note("Check for ETL updates")
        _config_fabric(settings.fabric)
        with cd("~/ActiveData-ETL/"):
            result = run("git pull origin etl")
            if result.find("Already up-to-date.") != -1:
                Log.note("No change required")
                return
            sudo("supervisorctl restart etl")

    except Exception as e:
        Log.error("Problem with checking for ETL updates", e)
    finally:
        Log.stop()

if __name__ == "__main__":
    main()
