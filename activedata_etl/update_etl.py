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

from future.utils import text_type
import datetime

from boto import ec2 as boto_ec2
from boto.ec2 import cloudwatch
from fabric.context_managers import cd
from fabric.operations import run, sudo
from fabric.state import env

from mo_dots import unwrap, wrap
from mo_dots.objects import datawrap
from mo_logs import startup, constants
from mo_logs import Log
from mo_collections import UniqueIndex


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


def _config_fabric(connect, instance):
    if not instance.ip_address:
        Log.error("Expecting an ip address for {{instance_id}}", instance_id=instance.id)

    for k, v in connect.items():
        env[k] = v
    env.host_string = instance.ip_address
    env.abort_exception = Log.error


def _refresh_etl(instance, settings, conn):
    cpu_percent = get_cpu(conn, instance)
    Log.note("Reset {{instance_id}} (name={{name}}, cpu={{cpu|percent}}) at {{ip}}", instance_id=instance.id, name=instance.tags["Name"], ip=instance.ip_address, cpu=cpu_percent/100)


    _config_fabric(settings.fabric, instance)
    # sudo("pip install pympler")
    sudo("rm -fr /tmp/grcov*")
    with cd("~/ActiveData-ETL/"):
        result = run("git pull origin etl")
        sudo("pip install -r requirements.txt")
        if result.find("Already up-to-date.") != -1:
            Log.note("No change required")
            if cpu_percent > 50:
                return
            Log.note("Low CPU implies problem, restarting anyway")
        sudo("supervisorctl restart all")


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
        for i in instances:
            try:
                _refresh_etl(i, settings, cw)
            except Exception as e:
                ec2_conn.terminate_instances([i.id])
                Log.warning("Problem resetting {{instance}}, TERMINATED!", instance=i.id, cause=e)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

