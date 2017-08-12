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

from boto import ec2 as boto_ec2
from fabric.api import settings as fabric_settings
from fabric.context_managers import cd, hide
from fabric.operations import run, put, sudo
from fabric.state import env

from mo_dots import unwrap, wrap
from mo_dots.objects import datawrap
from pyLibrary.aws import aws_retry
from mo_logs import startup, constants
from mo_logs import Log
from mo_files import File
from mo_collections import UniqueIndex
from mo_threads import Till


@aws_retry
def _get_managed_spot_requests(ec2_conn, name):
    output = wrap([datawrap(r) for r in ec2_conn.get_all_spot_instance_requests() if not r.tags.get("Name") or r.tags.get("Name").startswith(name)])
    return output


@aws_retry
def _get_managed_instances(ec2_conn, name):
    requests = UniqueIndex(["instance_id"], data=_get_managed_spot_requests(ec2_conn, name).filter(lambda r: r.instance_id != None))
    reservations = ec2_conn.get_all_instances()

    output = []
    for res in reservations:
        for instance in res.instances:
            if instance.tags.get('Name', '').startswith(name) and instance._state.name == "running":
                instance.request = requests[instance.id]
                output.append(datawrap(instance))
    return wrap(output)


def _config_fabric(connect, instance):
    if not instance.ip_address:
        Log.error("Expecting an ip address for {{instance_id}}", instance_id=instance.id)

    for k, v in connect.items():
        env[k] = v
    env.host_string = instance.ip_address
    env.abort_exception = Log.error


def _start_es():
    # KILL EXISTING "python27" PROCESS, IT MAY CONSUME TOO MUCH MEMORY AND PREVENT STARTUP
    with hide('output'):
        with fabric_settings(warn_only=True):
            run("ps -ef | grep python27 | grep -v grep | awk '{print $2}' | xargs kill -9")
    (Till(seconds=5)).wait()

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

def _disable_oom_on_es():
    with cd("/home/ec2-user"):
        run("mkdir -p temp")
    with cd("/home/ec2-user/temp"):
        processes = sudo("ps -eo pid,command | grep java")
        candidates = [
            line
            for line in processes.split("\n")
            if line.find("/usr/java/default/bin/java -Xms") != -1 and line.find("org.elasticsearch.bootstrap.Elasticsearch") != -1
        ]
        if not candidates:
            Log.error("Expecting to find some hint of Elasticsearch running")
        elif len(candidates) > 1:
            Log.error("Fond more than one Elasticsearch running, not sure what to do")

        pid = candidates[0].split(" ")[0].strip()
        run("echo -16 > oom_adj")
        sudo("sudo cp oom_adj /proc/" + pid + "/oom_adj")


def _refresh_indexer():
    _disable_oom_on_es()
    with cd("/home/ec2-user/ActiveData-ETL/"):
        sudo("pip install -r requirements.txt")
        result = run("git pull origin push-to-es")
        if result.find("Already up-to-date.") != -1:
            Log.note("No change required")
        else:
            # RESTART ANYWAY, SO WE USE LATEST INDEX
            with fabric_settings(warn_only=True):
                sudo("supervisorctl restart push_to_es")


def _start_supervisor():
    put("~/code/SpotManager/examples/config/es_supervisor.conf", "/etc/supervisord.conf", use_sudo=True)

    # START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
    with fabric_settings(warn_only=True):
        sudo("supervisord -c /etc/supervisord.conf")

    sudo("supervisorctl reread")
    sudo("supervisorctl update")


def _run_remote(command, name):
    File("./results/temp/" + name + ".sh").write("nohup " + command + " >& /dev/null < /dev/null &\nsleep 20")
    put("./results/temp/" + name + ".sh", "" + name + ".sh")
    run("chmod u+x " + name + ".sh")
    run("./" + name + ".sh")


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        aws_args = dict(
            region_name=settings.aws.region,
            aws_access_key_id=unwrap(settings.aws.aws_access_key_id),
            aws_secret_access_key=unwrap(settings.aws.aws_secret_access_key)
        )
        ec2_conn = boto_ec2.connect_to_region(**aws_args)

        instances = _get_managed_instances(ec2_conn, settings.name)

        for i in instances:
            try:
                Log.note("Reset {{instance_id}} ({{name}}) at {{ip}}", instance_id=i.id, name=i.tags["Name"], ip=i.ip_address)
                _config_fabric(settings.fabric, i)
                _refresh_indexer()
            except Exception as e:
                Log.warning(
                    "could not refresh {{instance_id}} ({{name}}) at {{ip}}",
                    instance_id=i.id,
                    name=i.tags["Name"],
                    ip=i.ip_address,
                    cause=e
                )
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

