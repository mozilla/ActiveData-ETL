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
from fabric.operations import get, sudo
from fabric.state import env
from mo_collections import UniqueIndex
from mo_dots import unwrap, wrap, coalesce
from mo_files import File, TempDirectory
from mo_logs import Log, strings
from mo_logs import startup, constants
from mo_times.durations import SECOND, ZERO

from mo_dots.objects import datawrap
from mo_times import Date, HOUR, Duration, DAY, MINUTE
from pyLibrary.aws import aws_retry

num_restarts = 1

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


def _find_oom(instance):
    with TempDirectory() as temp:
        get("/data1/logs/es.log", temp.abspath)
        last_restart_time = _get_es_restart_time(instance)
        found_oom = False
        for line in File.new_instance(temp, "es.log"):
            if "java.lang.OutOfMemoryError" in line:
                # Log.note("{{line}}", line=line)
                found_oom = True
            if found_oom:
                try:
                    oom_timestamp = Date(strings.between(line, "[", "]").split(",")[0])
                    if oom_timestamp:
                        found_oom = False
                    if oom_timestamp > last_restart_time:
                        # IT IS GOOD TO BOUNCE A ES NODE IF IT HAS HAD A OOM
                        Log.note("OOM at {{timestamp}} on {{instance_id}} ({{name}}) at {{ip}}", timestamp=oom_timestamp, instance_id=instance.id, name=instance.tags["Name"], ip=instance.ip_address)
                        _restart_es(instance)
                        return
                except Exception:
                    pass


def _get_es_restart_time(instance):
    now = Date.now()
    result = sudo("supervisorctl status")
    for r in result.split("\n"):
        try:
            if r.startswith("es"):
                days = int(coalesce(strings.between(r, "uptime ", " days"), "0"))
                duration = sum((b*int(a) for a, b in zip(strings.right(r.strip(), 8).split(":"), [HOUR, MINUTE, SECOND])), ZERO)
                last_restart_time = now-days*DAY-duration
                return last_restart_time
        except Exception:
            pass


def _restart_es(instance):
    global num_restarts

    if num_restarts <= 0:
        return

    Log.warning("Restart ES because of OoM: {{instance_id}} ({{name}}) at {{ip}}", instance_id=instance.id, name=instance.tags["Name"], ip=instance.ip_address)
    num_restarts -= 1
    sudo("supervisorctl restart es")


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
            if num_restarts <= 0:
                Log.note("No more restarts, exiting")
                return

            try:
                Log.note("Look for OOM {{instance_id}} ({{name}}) at {{ip}}", instance_id=i.id, name=i.tags["Name"], ip=i.ip_address)
                _config_fabric(settings.fabric, i)
                _find_oom(i)
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

