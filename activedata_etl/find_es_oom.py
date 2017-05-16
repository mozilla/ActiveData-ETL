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
from mo_dots import unwrap, wrap
from mo_files import File, TempDirectory
from mo_logs import Log, strings
from mo_logs import startup, constants

from mo_dots.objects import datawrap
from mo_times import Date, HOUR
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


def _config_fabric(connect, instance):
    if not instance.ip_address:
        Log.error("Expecting an ip address for {{instance_id}}", instance_id=instance.id)

    for k, v in connect.items():
        env[k] = v
    env.host_string = instance.ip_address
    env.abort_exception = Log.error


def _find_oom():
    with TempDirectory() as temp:
        get("/data1/logs/es.log", temp.abspath)
        found_oom = False
        for line in File.new_instance(temp, "es.log"):
            if "java.lang.OutOfMemoryError" in line:
                # Log.note("{{line}}", line=line)
                found_oom = True
            if found_oom:
                try:
                    timestamp = Date(strings.between(line, "[", "]").split(",")[0])
                    if timestamp:
                        found_oom = False
                    if timestamp and timestamp > Date.now()-2*HOUR:
                        Log.note("OOM: {{timestamp}}", timestamp=timestamp)
                        _restart_es()
                        break
                except Exception as e:
                    pass


def _restart_es():
    result = sudo("supervisorctl status")
    for r in result.split("\n"):
        try:
            if r.startswith("es"):
                days = int(strings.between(r, "uptime", "days").strip())
                if days > 1:
                    Log.alert("RESTART ES")
                    # sudo("supervisorctl restart es")
        except Exception:
            pass

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
                Log.note("Look for OOM {{instance_id}} ({{name}}) at {{ip}}", insance_id=i.id, name=i.tags["Name"], ip=i.ip_address)
                _config_fabric(settings.fabric, i)
                _find_oom()
            except Exception as e:
                Log.warning(
                    "could not refresh {{instance_id}} ({{name}}) at {{ip}}",
                    insance_id=i.id,
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

