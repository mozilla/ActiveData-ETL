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
from math import log10

import boto
from boto.ec2.networkinterface import NetworkInterfaceSpecification, NetworkInterfaceCollection
from boto.ec2.spotpricehistory import SpotPriceHistory
from boto.utils import ISO8601
from fabric.api import settings as fabric_settings
from fabric.context_managers import cd
from fabric.contrib import files as fabric_files
from fabric.operations import run, sudo, put
from fabric.state import env
from pyLibrary import aws

from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, dictwrap, coalesce
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.queries.expressions import CODE
from pyLibrary.strings import between
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import WEEK, DAY, HOUR


MIN_UTILITY_PER_DOLLAR = 8 * 5  # 8cpu per dollar (on demand price) multiply by expected 5x savings


class SpotManager(object):
    @use_settings
    def __init__(self, settings):
        self.settings = settings
        self.conn = boto.ec2.connect_to_region(
            region_name=settings.aws.region,
            aws_access_key_id=settings.aws.aws_access_key_id,
            aws_secret_access_key=settings.aws.aws_secret_access_key
        )

    def update_spot_requests(self, utility_required, config):
        """

        """

        # how many do we have?
        requests = list(map(dictwrap, self.conn.get_all_spot_instance_requests()))

        prices = self.pricing()

        #how many failed?

        #how many pending?
        pending = qb.filter(requests, {"terms": {"status.code": PENDING_STATUS_CODES}})
        not_available = qb.run({
            "from": requests,
            "select": ["type", "availability_zone"],
            "where": {"terms": {"status.code": TERMINATED_STATUS_CODES - RETRY_STATUS_CODES}}
        }).data
        running = qb.filter(requests, {"terms": {"status.code": RUNNING_STATUS_CODES}})

        current_utility = sum(map(lambda x: utility_lookup[x.launch_specification.instance_type], pending + running))

        new_utility = utility_required - current_utility
        if new_utility < 1:
            return

        #how many are too expensive?

        usable_prices = filter(lambda p: (p.type, p.availability_zone) not in not_available, prices)

        utility_per_dollar = MIN_UTILITY_PER_DOLLAR

        #what new spot requests are required?
        while new_utility > 1:
            for p in usable_prices:
                max_bid = p.type.utility / utility_per_dollar
                mid_bid = coalesce(p.higher_price, max_bid)
                min_bid = p.price_80
                num = Math.floor(new_utility / p.type.utility)
                if num == 1:
                    min_bid = mid_bid
                    price_interval = 0
                else:
                    #mid_bid = coalesce(mid_bid, max_bid)
                    price_interval = (mid_bid - min_bid) / (num - 1)

                for i in range(num):
                    bid = min_bid + (i * price_interval)

                    self.conn.request_spot_instances(
                        price=bid,
                        image_id='ami-5189a661',
                        count=1,
                        type='one-time',
                        valid_from=None,
                        valid_until=None,
                        launch_group=None,
                        availability_zone_group=p.availability_zone,
                        key_name='aws-pulse-logger',
                        security_groups=None,
                        user_data=None,
                        addressing_type=None,
                        instance_type=p.type.instance_type,
                        placement=None,
                        kernel_id=None,
                        ramdisk_id=None,
                        monitoring_enabled=False,
                        subnet_id=None,
                        placement_group=None,
                        block_device_map=None,
                        instance_profile_arn=None,
                        instance_profile_name=None,
                        security_group_ids=None,
                        ebs_optimized=False,
                        network_interfaces=NetworkInterfaceCollection(NetworkInterfaceSpecification(
                            subnet_id='subnet-b7c137ee',
                            groups=['sg-bb542fde'],
                            associate_public_ip_address=True
                        )),
                        dry_run=False
                    )
                    new_utility -= p.type.utility

    def pricing(self):
        prices = []
        for instance_type in config.instance_type:
            Log.note("get pricing for {{instance_type}}", {"instance_type": instance_type})
            resultset = self.conn.get_spot_price_history(
                product_description="Linux/UNIX",
                instance_type=instance_type,
                availability_zone = "us-west-2c",
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

        hourly_pricing = qb.run({
            "from": {
                # AWS PRICING ONLY SENDS timestamp OF CHANGES, MATCH WITH NEXT INSTANCE
                "from": prices,
                "window": {
                    "name": "expire",
                    "value": CODE("coalesce(rows[rownum+1].timestamp, Date.eod())"),
                    "edges": ["availability_zone", "instance_type"],
                    "sort": "timestamp"
                }
            },
            "edges": [
                "availability_zone",
                "instance_type",
                {
                    "name": "time",
                    "range": {"min": "timestamp", "max": "expire", "mode": "inclusive"},
                    "domain": {"type": "time", "min": (Date.now() - DAY).floor(HOUR), "max": Date.now().floor(HOUR), "interval": "hour"}
                }
            ],
            "select": [
                {"value": "price", "aggregate": "max"},
                {"aggregate": "count"}
            ],
            "window": {
                "name": "current_price", "value": CODE("rows.last().price"), "edges": ["availability_zone", "instance_type"], "sort": "time",
            }
        }).data

        bid80 = qb.run({
            "from": hourly_pricing,
            "edges": [
                {
                    "value": "availability_zone",
                    "allowNulls": False
                },
                {
                    "name": "type",
                    "value": "instance_type",
                    "allowNulls": False,
                    "domain": {"type": "set", "key": "instance_type", "partitions": config}
                }
            ],
            "select": [
                {"name": "price_80", "value": "price", "aggregate": "percentile", "percentile": 0.80},
                {"name": "max_price", "value": "price", "aggregate": "max"},
                {"aggregate": "count"},
                {"value": "current_price", "aggregate": "one"},
                {"name": "all_price", "value": "price", "aggregate": "list"}
            ],
            "window": [
                {"name": "estimated_value", "value": {"div": ["type.utility", "price_80"]}},
                {"name": "higher_price", "value": lambda row: find_higher(row.all_price, row.price_80)}
            ]
        })

        prices = qb.run({
            "from": bid80.data,
            "sort": {"value": "estimated_value", "sort": -1}
        })

        return prices.data

    def setup_instance(self, instance_id, cpu_count):
        reservations = self.conn.get_all_instances()
        instance = [i for r in reservations for i in r.instances if i.id==instance_id][0]
        instance.add_tag('Name', self.settings.ec2.instance.name)


        # env.use_ssh_config = True
        env.disable_known_hosts = True
        env.host_string = instance.ip_address
        env.port = 22
        env.user = self.settings.ec2.instance.user
        env.password = ""
        env.key_filename = File(self.settings.ec2.instance.key_filename).abspath

        # self.setup_etl_code()
        # self.add_private_file()
        self.setup_etl_supervisor(cpu_count)

    def setup_etl_code(self):
        sudo("sudo apt-get update")

        if not fabric_files.exists("/home/ubuntu/temp"):
            run("mkdir -p /home/ubuntu/temp")

            with cd("/home/ubuntu/temp"):
                # INSTALL FROM CLEAN DIRECTORY
                run("wget https://bootstrap.pypa.io/get-pip.py")
                sudo("python get-pip.py")

        if not fabric_files.exists("/home/ubuntu/TestLog-ETL"):
            with cd("/home/ubuntu"):
                sudo("apt-get -y install git-core")
                run("git clone https://github.com/klahnakoski/TestLog-ETL.git")

        with cd("/home/ubuntu/TestLog-ETL"):
            run("git checkout etl")
            # pip install -r requirements.txt HAS TROUBLE IMPORTING SOME LIBS
            sudo("pip install MozillaPulse")
            sudo("pip install boto")
            sudo("pip install requests")
            sudo("apt-get -y install python-psycopg2")

    def setup_etl_supervisor(self, cpu_count):
        # INSTALL supervsor
        sudo("apt-get install -y supervisor")
        with fabric_settings(warn_only=True):
            run("service supervisor start")

        # READ LOCAL CONFIG FILE, ALTER IT FOR THIS MACHINE RESOURCES, AND PUSH TO REMOTE
        conf_file = File("./resources/supervisor/etl.conf")
        content = conf_file.read_bytes()
        find = between(content, "numprocs=", "\n")
        content = content.replace("numprocs=" + find + "\n", "numprocs=" + str(cpu_count*2) + "\n")
        File("./resources/supervisor/etl.conf.alt").write_bytes(content)
        sudo("rm -f /etc/supervisor/conf.d/etl.conf")
        put("./resources/supervisor/etl.conf.alt", '/etc/supervisor/conf.d/etl.conf', use_sudo=True)
        run("mkdir -p /home/ubuntu/TestLog-ETL/results/logs")

        # POKE supervisor TO NOTICE THE CHANGE
        sudo("supervisorctl reread")
        sudo("supervisorctl update")

    def add_private_file(self):
        put('~/private.json', '/home/ubuntu')
        with cd("/home/ubuntu"):
            run("chmod o-r private.json")




def find_higher(candidates, reference):
    """
    RETURN ONE PRICE HIGHER THAN reference
    """
    output = wrap([c for c in candidates if c > reference])[0]
    return output




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

utility_lookup = {}
for c in config:
    c.utility = min(c.cpu, 8)
    utility_lookup[c.instance_type]=c.utility



TERMINATED_STATUS_CODES = set([
    "capacity-oversubscribed",
    "capacity-not-available",
    "instance-terminated-capacity-oversubscribed",
    "bad-parameters"
])
RETRY_STATUS_CODES = set([
    "instance-terminated-by-price",
    "price-too-low",
    "bad-parameters",
    "canceled-before-fulfillment",
    "instance-terminated-by-user"
])
PENDING_STATUS_CODES = set([
    "pending-evaluation",
    "pending-fulfillment"
])
RUNNING_STATUS_CODES = set([
    "fulfilled",
])



def main():
    """
    CLEAR OUT KEYS FROM BUCKET BY RANGE, OR BY FILE
    """
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        m = SpotManager(settings)

        queue = aws.Queue(settings.work_queue)
        pending = len(queue)
        # DUE TO THE LARGE VARIABILITY OF WORK FOR EACH ITEM IN QUEUE, WE USE LOG TO SUPRESS
        utility_required = min(1, log10(min(pending, 1)) * 10)

        m.update_spot_requests(utility_required, config)
        # m.setup_instance("i-559dc19c", cpu_count=1)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
