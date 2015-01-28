# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from boto.ec2 import connect_to_region
from pyLibrary.debugs import constants, startup
from pyLibrary.debugs.logs import Log
from pyLibrary.meta import use_settings


@use_settings
def make_worker(
    region,
    aws_access_key_id,
    aws_secret_access_key,
    settings=None
):

    # GET IAM ROLE

    # MAKE INSTANCE
    conn = connect_to_region(region)
    conn.run_instances(
        image_id='ami-3d50120d',
        key_name='aws-pulse-logger',
        instance_type='t2.medium',
        subnet_id='subnet-b7c137ee',
        security_groups=['sg-bb542fde'],
        instance_initiated_shutdown_behavior='terminate',
        instance_profile_arn='active-data',
        instance_profile_name='autogen test instance',
        network_interfaces='vpc-f97deb92'
    )




    # LOAD SOFTWARE
    # START
    # MONITOR PROGRESS
    # DISCONNECT
    # TERMINATE INSTANCE


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        constants.set(settings.constants)

        make_worker()
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
