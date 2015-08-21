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

from boto import ec2 as boto_ec2
from fabric.context_managers import cd
from fabric.operations import run, sudo
from fabric.state import env

from pyLibrary.debugs import startup, constants
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import unwrap, wrap
from pyLibrary.dot.objects import dictwrap
from pyLibrary.queries.unique_index import UniqueIndex


def _get_managed_spot_requests(ec2_conn, name):
    output = wrap([dictwrap(r) for r in ec2_conn.get_all_spot_instance_requests() if not r.tags.get("Name") or r.tags.get("Name").startswith(name)])
    return output


def _get_managed_instances(ec2_conn, name):
    requests = UniqueIndex(["instance_id"], data=_get_managed_spot_requests(ec2_conn, name).filter(lambda r: r.instance_id != None))
    reservations = ec2_conn.get_all_instances()

    output = []
    for res in reservations:
        for instance in res.instances:
            if instance.tags.get('Name', '').startswith(name) and instance._state.name == "running":
                instance.request = requests[instance.id]
                output.append(dictwrap(instance))
    return wrap(output)


def _config_fabric(connect, instance):
    if not instance.ip_address:
        Log.error("Expecting an ip address for {{instance_id}}", instance_id=instance.id)

    for k, v in connect.items():
        env[k] = v
    env.host_string = instance.ip_address
    env.abort_exception = Log.error


def _refresh_etl():
    with cd("~/TestLog-ETL/"):
        result = run("git pull origin etl")
        if result.find("Already up-to-date.") != -1:
            Log.note("No change required")
            return
        sudo("supervisorctl restart all")


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
            Log.note("Reset {{instance_id}} ({{name}}) at {{ip}}", insance_id=i.id, name=i.tags["Name"], ip=i.ip_address)
            _config_fabric(settings.fabric, i)
            try:
                _refresh_etl()  # TODO: UPON FAILURE, TERMINATE INSTANCE AND SPOT REQUEST
            except Exception, e:
                ec2_conn.terminate_instances([i.id])
                Log.warning("Problem resetting {{instance}}, terminated", instance=i.id, cause=e)
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
