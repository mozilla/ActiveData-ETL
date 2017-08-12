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

from future import text_type
from activedata_etl import key2etl, etl2key
from activedata_etl.transforms.unittest_logs_to_sink import accumulate_logs
from mo_dots import Data, wrap, set_default
from mo_logs import Log, machine_metadata, strings
from mo_times import Date

DEBUG = False
DEBUG_SHOW_LINE = True
DEBUG_SHOW_NO_LOG = False
PARSE_TRY = True
SINGLE_URL = None


def process(source_key, source, destination, resources, please_stop=None):
    test_results = accumulate_logs(source_key, resources.url, source.read_lines(), please_stop)
    parent_source = key2etl(source_key)

    run_info = Data()
    for k, v in test_results.run_info.leaves():
        run_info[k.lower()] = v

    run_info.packages = [{"name":k, "version":v} for k,v in run_info.packages.items()]
    run_info.plugins = [{"name":k, "version":v} for k,v in run_info.plugins.items()]

    ## READ SOURCE FILE
    normalized = []
    for line_number, record in enumerate(test_results.tests):
        test_path = record.test.split("::")
        test_name = test_path[-1].split("[")[0]
        option = strings.between(test_path[-1], "[", "]")
        record.time /= 1000

        normalized.append({
            "result": record,
            "test": {
                "full_name": record.test,
                "file": test_path[0],
                "name": test_name,
                "suite": test_path[1],
                "option": option
            },
            "run": set_default(
                {"stats": test_results.stats},
                run_info
            ),
            "etl": {
                "id": line_number,
                "source": parent_source,
                "type": "join",
                "timestamp": Date.now(),
                "machine": machine_metadata
            }

        })

    return destination.extend({"id": etl2key(n.etl), "value": n} for n in wrap(normalized))
