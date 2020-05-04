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

import datetime

from boto import ec2 as boto_ec2
from boto.ec2 import cloudwatch

from jx_python import jx
from mo_collections import UniqueIndex
from mo_dots import unwrap, wrap
from mo_dots.objects import datawrap
from mo_fabric import Connection
from mo_files import File
from mo_logs import Log, Except
from mo_logs import startup, constants
from mo_threads import MAIN_THREAD, Thread
from mo_times import Timer


def _get_managed_spot_requests(ec2_conn, name):
    output = wrap([datawrap(r) for r in ec2_conn.get_all_spot_instance_requests() if not r.tags.get("Name") or r.tags.get("Name").startswith(name)])
    return output


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


def _refresh_etl(instance, settings, cw, ec2_conn, please_stop):
    try:
        with Connection(host=instance.ip_address, kwargs=settings.fabric) as conn:
            # _update_ssh(conn)

            cpu_percent = get_cpu(cw, instance)
            Log.note(
                "Reset {{instance_id}} (name={{name}}, cpu={{cpu|percent}}) at {{ip}}",
                instance_id=instance.id,
                name=instance.tags["Name"],
                ip=instance.ip_address,
                cpu=cpu_percent/100
            )

            conn.sudo("rm -fr /tmp/grcov*")
            with conn.cd("~/ActiveData-ETL/"):
                result = conn.run("git pull origin etl", warn=True)
                if "Already up-to-date." in result or "Already up to date." in result:
                    Log.note("No change required")
                    if cpu_percent > 50:
                        return
                    Log.note("{{ip}} - Low CPU implies problem, restarting anyway", ip=instance.ip_address)
                conn.sudo("supervisorctl restart all")
    except Exception as e:
        e = Except.wrap(e)
        if "No authentication methods available" in e:
            Log.warning("Missing private key to connect?", cause=e)
        else:
            ec2_conn.terminate_instances([instance.id])
            Log.warning("Problem resetting {{instance}}, TERMINATED!", instance=instance.id, cause=e)

def _update_ssh(conn):
    public_key = File("d:/activedata.pub.ssh")
    with conn.cd("/home/ec2-user"):
        conn.put(public_key, ".ssh/authorized_keys")
        conn.run("chmod 600 .ssh/authorized_keys")


def get_cpu(conn, i):
    stats = list(conn.get_metric_statistics(
        period=600,
        start_time=datetime.datetime.utcnow() - datetime.timedelta(seconds=600),
        end_time=datetime.datetime.utcnow(),
        metric_name='CPUUtilization',
        namespace='AWS/EC2',
        statistics='Average',
        dimensions={'InstanceId': [i.id]},
        unit='Percent'
    ))
    if len(stats) == 0:
        return 100  # OPTIMISTIC

    cpu_percent = stats[-1]['Average']
    return cpu_percent


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
        cw = cloudwatch.connect_to_region(**aws_args)

        instances = _get_managed_instances(ec2_conn, settings.name)
        if not instances:
            Log.alert("No instances found. DONE.")
            return
        for g, members in jx.chunk(instances, size=40):
            # TODO: A THREAD POOL WOULD BE NICE
            # pool = Thread.pool(40)
            # for i in instances: pool("refresh etl", _refresh_etl, i, settings, cw)
            with Timer("block of {{num}} threads", {"num": len(members)}):
                threads = [
                    Thread.run("refresh etl", _refresh_etl, i, settings, cw, ec2_conn)
                    for i in members
                ]
                for t in threads:
                    t.join()
    except Exception as e:
        Log.warning("Problem with etl! Shutting down.", cause=e)
    finally:
        MAIN_THREAD.stop()


if __name__ == "__main__":
    main()

