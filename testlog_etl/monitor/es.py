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

from fabric.api import settings as fabric_settings
from fabric.context_managers import cd, shell_env, hide
from fabric.operations import run, put, sudo
from fabric.state import env

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.env.files import File
from pyLibrary.thread.threads import Thread


def _config_fabric(connect):
    for k, v in connect.items():
        Log.note("set {{key}}={{value}}", key=k, value=v)
        env[k] = v
    env.abort_exception = Log.error


def _start_es():
    # KILL EXISTING "python27" PROCESS, IT MAY CONSUME TOO MUCH MEMORY AND PREVENT STARTUP
    with hide('output'):
        with fabric_settings(warn_only=True):
            run("ps -ef | grep python27 | grep -v grep | awk '{print $2}' | xargs kill -9")
    Thread.sleep(seconds=5)

    File("./results/temp/start_es.sh").write("nohup ./bin/elasticsearch >& /dev/null < /dev/null &\nsleep 20")
    with cd("/home/ec2-user/"):
        put("./results/temp/start_es.sh", "start_es.sh")
        run("chmod u+x start_es.sh")

    with cd("/usr/local/elasticsearch/"):
        sudo("/home/ec2-user/start_es.sh")


def _es_up():
    """
    ES WILL BE LIVE WHEN THIS RETURNS
    """

    #SEE IF JAVA IS RUNNING
    pid = run("ps -ef | grep java | grep -v grep | awk '{print $2}'")
    if not pid:
        with hide('output'):
            log = run("tail -n100 /data1/logs/active-data.log")
        Log.warning("ES not Running:\n{{log|indent}}", log=log)

        _start_es()
        return

    #SEE IF IT IS RESPONDING
    result = run("curl http://localhost:9200/unittest/_search -d '{\"fields\":[\"etl.id\"],\"query\": {\"match_all\": {}},\"from\": 0,\"size\": 1}'")
    if result.find("\"_shards\":{\"total\":24,") == -1:
        # BAD RESPONSE, KILL JAVA
        with hide('output'):
            log = run("tail -n100 /data1/logs/active-data.log")
        Log.warning("ES Not Responsive:\n{{log|indent}}", log=log)

        sudo("kill -9 " + pid)
        _start_es()
        return


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)
        Log.note("Monitor ES")
        _config_fabric(settings.fabric)
        _es_up()
    except Exception, e:
        Log.error("Problem with monitoring ES", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
