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

from jx_python import jx
from mo_collections import UniqueIndex
from mo_dots import unwrap, wrap
from mo_dots.objects import datawrap, DataObject
from mo_fabric import Connection
from mo_logs import Log, startup, constants
from mo_threads import Thread
from pyLibrary.aws import aws_retry


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


def _disable_oom_on_es(conn):
    with conn.warn_only():
        conn.sudo("supervisorctl start es")

    with conn.cd("/home/ec2-user"):
        conn.run("mkdir -p temp")
    with conn.cd("/home/ec2-user/temp"):
        processes = conn.sudo("ps -eo pid,command | grep java")
        candidates = [
            line
            for line in processes.split("\n")
            if "/usr/java/default/bin/java -Xms" in line and "org.elasticsearch.bootstrap.Elasticsearch" in line
        ]
        if not candidates:
            Log.error("Expecting to find some hint of Elasticsearch running")
        elif len(candidates) > 1:
            Log.error("Fond more than one Elasticsearch running, not sure what to do")

        pid = candidates[0].strip().split(" ")[0].strip()
        conn.run("echo -16 > oom_adj")
        conn.sudo("sudo cp oom_adj /proc/" + pid + "/oom_adj")


def _refresh_indexer(config, instance, please_stop):
    Log.note(
        "Reset {{instance_id}} ({{name}}) at {{ip}}",
        instance_id=instance.id,
        name=instance.tags["Name"],
        ip=instance.ip_address
    )
    try:
        with Connection(config, host=instance.ip_address) as conn:
            with conn.cd("/usr/local/elasticsearch"):
                conn.sudo("rm -f java*.hprof")

            _disable_oom_on_es(conn)
            with conn.cd("/home/ec2-user/ActiveData-ETL/"):
                result = conn.run("git pull origin push-to-es6")
                if "Already up-to-date." in result:
                    Log.note("No change required")
                else:
                    # RESTART ANYWAY, SO WE USE LATEST INDEX
                    conn.run("~/pypy/bin/pypy -m pip install -r requirements.txt")
                    with conn.warn_only():
                        conn.sudo("supervisorctl stop push_to_es:*")
                        conn.sudo("supervisorctl start push_to_es:00")
    except Exception as e:
        Log.warning(
            "could not refresh {{instance_id}} ({{name}}) at {{ip}}",
            instance_id=instance.id,
            name=instance.tags["Name"],
            ip=instance.ip_address,
            cause=e
        )


def _start_supervisor(conn):
    conn.put("~/code/SpotManager/examples/config/es_supervisor.conf", "/etc/supervisord.conf", use_sudo=True)

    # START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
    with conn.warn_only():
        conn.sudo("supervisord -c /etc/supervisord.conf")

    conn.sudo("supervisorctl reread")
    conn.sudo("supervisorctl update")


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

        for g, ii in jx.groupby(instances, size=1):
            threads = [
                Thread.run(i.name, _refresh_indexer, settings.fabric, i)
                for i in ii
            ]

            for t in threads:
                t.join()
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


