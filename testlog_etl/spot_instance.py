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

import boto
from boto.ec2.spotpricehistory import SpotPriceHistory
from boto.utils import ISO8601
from fabric.context_managers import cd
from fabric.operations import run, sudo, put

from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import WEEK, DAY


class SpotManager(object):
    @use_settings
    def __init__(self, region, aws_access_key_id, aws_secret_access_key, settings):
        self.settings = settings
        self.conn = boto.ec2.connect_to_region(
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

    def update_requests(self, num_required, config):
        """
        :return:
        """

        # how may do we need?

        #how many do we have?
        requests = self.conn.get_all_spot_instance_requests()

        #how many failed?

        #how many pending?

        #what new spot requests are required?
        num_new = 0
        for i in range(num_new):
            pass
            # self.conn.request_spot_instances(...)

            # self, price, image_id, count=1, type='one-time',
            # valid_from=None, valid_until=None,
            # launch_group=None, availability_zone_group=None,
            # key_name=None, security_groups=None,
            # user_data=None, addressing_type=None,
            # instance_type='m1.small', placement=None,
            # kernel_id=None, ramdisk_id=None,
            # monitoring_enabled=False, subnet_id=None,
            # placement_group=None,
            # block_device_map=None,
            # instance_profile_arn=None,
            # instance_profile_name=None,
            # security_group_ids=None,
            # ebs_optimized=False,
            # network_interfaces=None, dry_run=False):


    def pricing(self):
        prices = []
        for instance_type in config.instance_type:
            Log.note("get pricing for {{instance_type}}", {"instance_type": instance_type})
            resultset = self.conn.get_spot_price_history(
                product_description="Linux/UNIX",
                instance_type=instance_type,
                start_time=(Date.today() - WEEK).format(ISO8601)
            )
            prices.extend([
                {
                    "availability_zone": p.availability_zone,
                    "instance_type": p.instance_type,
                    "price": p.price,
                    "product_description": p.product_description,
                    "region": p.region.name,
                    "timestamp": Date(p.timestamp)
                }
                for p in resultset
            ])

        bid80 = qb.run({
            "from": {
                "from": prices,
                "window": {"name": "expire", "value": "nvl(rows[rownum+1].timestamp, Date.eod())", "edges": ["availability_zone", "instance_type"], "sort": "timestamp"}
            },
            "edges": [
                "availability_zone",
                "instance_type",
                {
                    "name": "time",
                    "range": {"min": "timestamp", "max": "expire"},
                    "domain": {"type": "time", "min": Date.today()-WEEK, "max": Date.eod(), "interval": "hour"}
                }
            ],
            "select": [
                {"value": "price", "aggregate": "percentile", "percentile": 0.80},
                {"aggregate": "count"}
            ]
        })

        west = qb.filter(prices, {"prefix": {"availability_zone": "us-west"}})
        Log.note("prices\n{{prices|indent}}", {"prices": list(prices)})


    def setup_etl_node(self):

        run("mkdir /home/ubuntu/temp")
        with cd("/home/ubuntu/temp"):
            # INSTALL FROM CLEAN DIRECTORY
            run("wget https://bootstrap.pypa.io/get-pip.py")
            sudo("python get-pip.py")

        with cd("/home/ubuntu"):
            sudo("apt-get -y install git-core")
            sudo("git clone https://github.com/klahnakoski/TestLog-ETL.git")

        with cd("/home/ubuntu/TestLog-ETL"):
            run("git checkout etl")
            sudo("pip install -r requirements.txt")

    def setup_etl_supervisor(self):
        sudo("apt-get install -y supervisor")
        sudo("service supervisor start")

        run("mkdir -p /home/ubuntu/TestLog-ETL/results/logs")
        sudo("cp /home/ubuntu/TestLog-ETL/resources/supervisor/etl.conf /etc/supervisor/conf.d/")
        sudo("supervisorctl reread")
        sudo("supervisorctl update")

    def add_private_file(self):
        put('~/private.json', '/home/ubuntu')


config = wrap([
    # {"instance_type": "t2.micro", "cpu": 0.1},
    # {"instance_type": "t2.small", "cpu": 0.2},
    # {"instance_type": "t2.medium", "cpu": 0.4},

    {"instance_type": "m3.medium", "cpu": 1},
    {"instance_type": "m3.large", "cpu": 2},
    {"instance_type": "m3.xlarge", "cpu": 4},
    {"instance_type": "m3.2xlarge", "cpu": 8},

    # {"instance_type": "c4.large", "cpu": 2},
    # {"instance_type": "c4.xlarge", "cpu": 4},
    # {"instance_type": "c4.2xlarge", "cpu": 8},
    # {"instance_type": "c4.4xlarge", "cpu": 16},
    # {"instance_type": "c4.8xlarge", "cpu": 36},
    #
    # {"instance_type": "c3.large", "cpu": 2},
    # {"instance_type": "c3.xlarge", "cpu": 4},
    # {"instance_type": "c3.2xlarge", "cpu": 8},
    # {"instance_type": "c3.4xlarge", "cpu": 16},
    # {"instance_type": "c3.8xlarge", "cpu": 32},
    #
    # {"instance_type": "r3.large", "cpu": 2},
    # {"instance_type": "r3.xlarge", "cpu": 4},
    # {"instance_type": "r3.2xlarge", "cpu": 8},
    # {"instance_type": "r3.4xlarge", "cpu": 16},
    # {"instance_type": "r3.8xlarge", "cpu": 32}
])

for c in config:
    c.utility = max(c.cpu, 8)


def main():
    """
    CLEAR OUT KEYS FROM BUCKET BY RANGE, OR BY FILE
    """
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        m = SpotManager(settings.aws)
        m.pricing()
        m.update_requests(1, config)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
