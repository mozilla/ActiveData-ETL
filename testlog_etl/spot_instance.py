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

from pyLibrary import aws, convert
from pyLibrary.collections import SUM
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, dictwrap, coalesce, listwrap, unwrap, DictList
from pyLibrary.env.files import File
from pyLibrary.maths import Math
from pyLibrary.meta import use_settings
from pyLibrary.queries import qb
from pyLibrary.queries.expressions import CODE
from pyLibrary.strings import between
from pyLibrary.thread.threads import Lock, Thread
from pyLibrary.times.dates import Date
from pyLibrary.times.durations import DAY, HOUR, WEEK


BUDGET = 1.0  # DOLLARS PER HOUR
MIN_UTILITY_PER_DOLLAR = 8 * 10  # 8cpu per dollar (on demand price) multiply by expected 5x savings


class SpotManager(object):
    @use_settings
    def __init__(self, settings):
        self.settings = settings
        self.conn = boto.ec2.connect_to_region(
            region_name=settings.aws.region,
            aws_access_key_id=settings.aws.aws_access_key_id,
            aws_secret_access_key=settings.aws.aws_secret_access_key
        )
        self.price_locker = Lock()
        self.prices = None
        self._start_lifecycle_watcher()

    def _get_managed_instances(self):
        output =[]
        reservations = self.conn.get_all_instances()
        for res in reservations:
            for instance in res.instances:
                if instance.tags.get('Name', '').startswith(self.settings.ec2.instance.name):
                    output.append(dictwrap(instance))
        return wrap(output)



    def remove_extra_instances(self, spot_requests, utility_to_remove, prices):
        # FIND THE BIGGEST, MOST EXPENSIVE REQUESTS
        instances = self._get_managed_instances()

        for r in instances:
            r.markup = prices.filter(lambda x: x.type.instance_type == r.instance_type)[0]

        instances = qb.sort(instances, [
            {"value": "markup.type.cpu", "sort": -1},
            {"value": "markup.estimated_value", "sort": -1}
        ])

        # FIND COMBO THAT WILL SHUTDOWN WHAT WE NEED EXACTLY, OR MORE
        remove_list = []
        for acceptable_error in range(0, 8):
            remaining_utility = utility_to_remove
            remove_list = DictList()
            for s in instances:
                utility = coalesce(s.markup.type.utility, 0)
                if utility <= remaining_utility + acceptable_error:
                    remove_list.append(s)
                    remaining_utility -= utility
            if remaining_utility <= 0:
                break

        # SEND SHUTDOWN TO EACH INSTANCE
        for id in remove_list.id:
            self.teardown_instance(id)

        remove_requests = remove_list.spot_instance_request_id

        # TERMINATE INSTANCES
        self.conn.terminate_instances(instance_ids=remove_list.id)

        # TERMINATE SPOT REQUESTS
        self.conn.cancel_spot_instance_requests(request_ids=remove_requests)

        return -remaining_utility  # RETURN POSITIVE NUMBER IF TOOK AWAY TOO MUCH


    def update_spot_requests(self, utility_required, config):
        # how many do we have?
        prices = self.pricing()

        #DO NOT GO OVER BUDGET
        remaining_budget = BUDGET

        spot_requests = wrap([dictwrap(r) for r in self.conn.get_all_spot_instance_requests()])
        # instances = wrap([dictwrap(i) for r in self.conn.get_all_instances() for i in r.instances])

        # ADD UP THE CURRENT REQUESTED INSTANCES
        active = qb.filter(spot_requests, {"terms": {"status.code": RUNNING_STATUS_CODES | PENDING_STATUS_CODES}})
        # running = instances.filter(lambda i: i.id in active.instance_id and i._state.name == "running")
        current_spending = coalesce(SUM(self.price_lookup[r.launch_specification.instance_type].current_price for r in active), 0)
        remaining_budget -= current_spending

        current_utility = coalesce(SUM(self.price_lookup[r.launch_specification.instance_type].type.utility for r in active), 0)
        net_new_utility = utility_required - current_utility

        if net_new_utility < 1:  # ONLY REMOVE UTILITY IF WE NEED NONE
            net_new_utility += self.remove_extra_instances(spot_requests, -net_new_utility, prices)

        utility_per_dollar = MIN_UTILITY_PER_DOLLAR

        #what new spot requests are required?
        while net_new_utility > 1:
            for p in prices:
                max_bid = p.type.utility / utility_per_dollar
                mid_bid = Math.min(p.higher_price, max_bid)
                min_bid = p.price_80
                num = Math.floor(net_new_utility / p.type.utility)
                if num == 1:
                    min_bid = mid_bid
                    price_interval = 0
                else:
                    #mid_bid = coalesce(mid_bid, max_bid)
                    price_interval = (mid_bid - min_bid) / (num - 1)

                for i in range(num):
                    bid = min_bid + (i * price_interval)
                    if bid < p.current_price:
                        continue

                    self._request_spot_instance(
                        price=bid,
                        availability_zone_group=p.availability_zone,
                        instance_type=p.type.instance_type,
                        settings=self.settings.ec2.request
                    )
                    net_new_utility -= p.type.utility
                    remaining_budget -= p.current_price


    def _start_lifecycle_watcher(self):
        def worker(please_stop):
            self.pricing()

            while not please_stop:
                # spot_requests = wrap([dictwrap(r) for r in self.conn.get_all_spot_instance_requests()])
                # instances = wrap([dictwrap(i) for r in self.conn.get_all_instances() for i in r.instances])
                #
                # #INSTANCES THAT REQUIRE SETUP
                # please_setup = instances.filter(lambda i: i.id in spot_requests.instance_id and not i.tags.get("Name"))
                # for i in please_setup:
                #     try:
                #         p = self.price_lookup[i.instance_type]
                #         self.setup_instance(i.id, p.type.utility)
                #         i.add_tag('Name', self.settings.ec2.instance.name + " (running)")
                #     except Exception, e:
                #         pass

                Thread.sleep(seconds=5)

        Thread.run("lifecycle watcher", worker)

    @use_settings
    def _request_spot_instance(self, price, availability_zone_group, instance_type, settings=None):
        settings.network_interfaces = NetworkInterfaceCollection(
            *unwrap(NetworkInterfaceSpecification(**unwrap(s)) for s in listwrap(settings.network_interfaces))
        )
        settings.settings = None
        return self.conn.request_spot_instances(**unwrap(settings))

    def pricing(self):
        with self.price_locker:
            if self.prices:
                return self.prices

            prices = self._get_spot_prices_from_aws()

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
                        "domain": {"type": "time", "min": Date.now().floor(HOUR) - DAY, "max": Date.now().floor(HOUR), "interval": "hour"}
                    }
                ],
                "select": [
                    {"value": "price", "aggregate": "max"},
                    {"aggregate": "count"}
                ],
                "where": {"gt": {"timestamp": Date.now().floor(HOUR) - DAY}},
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

            output = qb.run({
                "from": bid80.data,
                "sort": {"value": "estimated_value", "sort": -1}
            })

            self.prices = output.data
            self.price_lookup = {p.type.instance_type: p for p in self.prices}
            return self.prices

    def _get_spot_prices_from_aws(self):
        try:
            content = File(self.settings.price_file).read()
            cache = convert.json2value(content, flexible=False, paths=False)
        except Exception, e:
            cache = DictList()

        most_recents = qb.run({
            "from": cache,
            "edges": ["instance_type"],
            "select": {"value": "timestamp", "aggregate": "max"}
        }).data


        prices = set(cache)
        for instance_type in config.instance_type:
            if most_recents:
                most_recent = most_recents[{"instance_type":instance_type}].timestamp
                if most_recent == None:
                    start_at = Date.today() - WEEK
                else:
                    start_at = Date(most_recent)
            else:
                start_at = Date.today() - WEEK
            Log.note("get pricing for {{instance_type}} starting at {{start_at}}", {
                "instance_type": instance_type,
                "start_at": start_at
            })

            next_token=None
            while True:
                resultset = self.conn.get_spot_price_history(
                    product_description="Linux/UNIX",
                    instance_type=instance_type,
                    availability_zone="us-west-2c",
                    start_time=start_at.format(ISO8601),
                    next_token=next_token
                )
                next_token = resultset.next_token

                for p in resultset:
                    prices.add(wrap({
                        "availability_zone": p.availability_zone,
                        "instance_type": p.instance_type,
                        "price": p.price,
                        "product_description": p.product_description,
                        "region": p.region.name,
                        "timestamp": Date(p.timestamp)
                    }))

                if not next_token:
                    break


        summary = qb.run({
            "from": prices,
            "edges": ["instance_type"],
            "select": {"value": "instance_type", "aggregate": "count"}
        })
        min_time = Math.MIN(wrap(list(prices)).timestamp)

        File(self.settings.price_file).write(convert.value2json(prices, pretty=True))
        return prices


    def teardown_instance(self, instance_id, ip_address=None):
        if ip_address is None:
            reservations = self.conn.get_all_instances()
            instance = [i for r in reservations for i in r.instances if i.id == instance_id][0]
            ip_address = instance.ip_address

        for k, v in self.settings.ec2.instance.connect.items():
            env[k] = v
        env.host_string = ip_address

        sudo("supervisorctl stop all")


    def setup_instance(self, instance_id, utility):
        cpu_count=int(round(utility))
        reservations = self.conn.get_all_instances()
        instance = [i for r in reservations for i in r.instances if i.id == instance_id][0]

        for k, v in self.settings.ec2.instance.connect.items():
            env[k] = v
        env.host_string = instance.ip_address

        self.setup_etl_code()
        self.add_private_file()
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
        content = content.replace("numprocs=" + find + "\n", "numprocs=" + str(cpu_count * 2) + "\n")
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
    # {"instance_type": "m3.xlarge", "cpu": 4},
    # {"instance_type": "m3.2xlarge", "cpu": 8},
    #
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
    # {"instance_type": "r3.8xlarge", "cpu": 32},
    #
    # {"instance_type": "d2.xlarge", "cpu": 4},
    # {"instance_type": "d2.2xlarge", "cpu": 8},
    # {"instance_type": "d2.4xlarge", "cpu": 16},
    # {"instance_type": "d2.8xlarge", "cpu": 36}
])

# THE ETL WORKLOAD IS LIMITED BY CPU
utility_lookup = {}
for c in config:
    c.utility = min(c.cpu, 8)
    utility_lookup[c.instance_type] = c.utility

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
        utility_required = max(1, log10(max(pending, 1)) * 10)

        m.update_spot_requests(utility_required, config)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
