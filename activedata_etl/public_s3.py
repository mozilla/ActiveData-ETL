# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from boto.s3 import connect_to_region

from mo_logs import startup, constants
from mo_logs import Log


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        all_users = 'http://acs.amazonaws.com/groups/global/AllUsers'
        conn = connect_to_region(
            settings.aws.region,
            aws_access_key_id=settings.aws.aws_access_key_id,
            aws_secret_access_key=settings.aws.aws_secret_access_key
        )

        bucket = conn.get_bucket('ekyle-unittest')

        for key in bucket:
            readable = False
            acl = key.get_acl()
            for grant in acl.acl.grants:
                if grant.permission == 'READ':
                    if grant.uri == all_users:
                        readable = True
            if not readable:
                key.make_public()
    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()



