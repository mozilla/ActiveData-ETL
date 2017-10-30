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

from activedata_etl.imports.task import minimize_task
from activedata_etl.transforms import EtlHeadGenerator
from mo_dots import listwrap, set_default
from mo_json import json2value, stream
from mo_logs import Log, machine_metadata
from mo_times.dates import Date
from pyLibrary.env import http
from pyLibrary.env.big_data import scompressed2ibytes


def process(source_key, source, destination, resources, please_stop=None):
    """
    READ pulse_block AND GET THE FILE -> COMPONENT MAPS
    """
    etl_header_gen = EtlHeadGenerator(source_key)

    existing_keys = destination.keys(prefix=source_key)
    for e in existing_keys:
        destination.delete_key(e)

    file_num = 0
    lines = list(source.read_lines())
    output = []

    for i, line in enumerate(lines):
        if please_stop:
            Log.error("Shutdown detected. Stopping early")

        task = json2value(line)
        etl = task.etl
        artifacts = listwrap(task.task.artifacts)

        if "public/components.json.gz" not in artifacts.name or "public/missing.json.gz" not in artifacts.name:
            continue

        minimize_task(task)

        # REVIEW THE ARTIFACTS, LOOK FOR
        for a in artifacts:
            if Date(a.expires) < Date.now():
                Log.note("Expired url: expires={{date}} url={{url}}", date=Date(a.expires), url=a.url)
                continue  # ARTIFACT IS GONE

            if "components.json.gz" in a.url:
                dest_key, dest_etl = etl_header_gen.next(etl, a.name)
                dest_etl.machine = machine_metadata
                dest_etl.url = a.url

                destination.extend(
                    normalize_property(source_key, data, task)
                    for data in stream.parse(
                        scompressed2ibytes(http.get(a.url).raw),
                        {"items": "."},
                        {"name", "value"}
                    )
                )

                file_num += 1
                output.append(dest_key)
            elif "missing.json.gz" in a.url:
                dest_key, dest_etl = etl_header_gen.next(etl, a.name)
                dest_etl.machine = machine_metadata
                dest_etl.url = a.url

                destination.extend(
                    normalize_missing(source_key, data, task)
                    for data in stream.parse(
                        scompressed2ibytes(http.get(a.url).raw),
                        "missing",
                        {"missing"}
                    )
                )

                file_num += 1
                output.append(dest_key)

    return output


def normalize_property(source_key, data, task):
    return set_default(
        {
            "file": {"name": data.name},
            "bug": {
                "product": data.value[0],
                "component": data.value[1]
            }

        },
        task
    )


def normalize_missing(source_key, data, task):
    return set_default(
        {
            "file": {"name": data}
        },
        task
    )
