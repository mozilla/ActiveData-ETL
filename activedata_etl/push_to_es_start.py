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
from fabric.operations import sudo
from fabric.state import env
from mo_dots import unwrap, wrap
from mo_logs import Log
from mo_logs import startup, constants

from mo_dots.objects import datawrap
from pyLibrary.aws import aws_retry


@aws_retry
def _get_managed_instances(ec2_conn, name):
    reservations = ec2_conn.get_all_instances()

    output = []
    for res in reservations:
        for instance in res.instances:
            if instance.tags.get('Name', '').startswith(name) and instance._state.name == "running":
                output.append(datawrap(instance))
    return wrap(output)


def _config_fabric(connect, instance):
    if not instance.ip_address:
        Log.error("Expecting an ip address for {{instance_id}}", instance_id=instance.id)

    for k, v in connect.items():
        env[k] = v
    env.host_string = instance.ip_address
    env.abort_exception = Log.error


def _stop_indexer():
    with fabric_settings(warn_only=True):
        # sudo("supervisorctl stop es")
        sudo("supervisorctl stop push_to_es:*")


def _start_indexer():
    with fabric_settings(warn_only=True):
        # sudo("supervisorctl start es")
        sudo("supervisorctl start push_to_es:00")


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
                _config_fabric(settings.fabric, i)
                Log.note("Start indexing {{instance_id}} ({{name}}) at {{ip}}", instance_id=i.id, name=i.tags["Name"], ip=i.ip_address)
                _start_indexer()
            except Exception as e:
                Log.warning("Problem with stopping", e)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

