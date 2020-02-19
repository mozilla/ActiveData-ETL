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

from activedata_etl import etl2key
from activedata_etl.imports.coverage_util import download_file
from activedata_etl.imports.task import minimize_task
from activedata_etl.transforms import EtlHeadGenerator, Transform
from jx_python import jx
from jx_python.expressions import jx_expression_to_function
from mo_dots import listwrap, wrap, Data, Null
from mo_files import TempFile
from mo_json import json2value, stream, value2json
from mo_logs import Log, machine_metadata
from mo_times import Timer
from mo_times.dates import Date
from mo_http.big_data import scompressed2ibytes


class ETL(Transform):

    def __init__(self, config):
        self.filter = jx_expression_to_function(config.task_filter)

    def __call__(self, source_key, source, destination, resources, please_stop=None):
        """
        READ pulse_block AND GET THE FILE -> COMPONENT MAPS
        """

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
            if not self.filter(task):
                continue

            etl = task.etl
            etl_header_gen = EtlHeadGenerator(etl2key(etl))
            artifacts = listwrap(task.task.artifacts)

            if "public/components.json.gz" not in artifacts.name or "public/missing.json.gz" not in artifacts.name:
                continue

            minimize_task(task)
            repo = task.repo
            repo.branch = None
            repo.push = None

            # REVIEW THE ARTIFACTS, LOOK FOR
            for a in artifacts:
                if Date(a.expires) < Date.now():
                    Log.note("Expired url: expires={{date}} url={{url}}", date=Date(a.expires), url=a.url)
                    continue  # ARTIFACT IS GONE

                if "components.json.gz" in a.url:
                    pass
                    dest_key, dest_etl = etl_header_gen.next(etl, name=a.name)
                    dest_etl.machine = machine_metadata
                    dest_etl.url = a.url

                    with TempFile() as tempfile:
                        Log.note("download {{url}}", url=a.url)
                        download_file(a.url, tempfile.abspath)
                        with open(tempfile.abspath, str("rb")) as fstream:
                            with Timer("process {{url}}", param={"url": a.url}):
                                destination.write_lines(
                                    dest_key,
                                    (
                                        value2json(normalize_property(source_key, data, repo, dest_etl, i, please_stop))
                                        for i, data in enumerate(stream.parse(
                                            scompressed2ibytes(fstream),
                                            {"items": "."},
                                            {"name", "value"}
                                        ))
                                    )
                                )

                    file_num += 1
                    output.append(dest_key)
                elif "missing.json.gz" in a.url:
                    dest_key, dest_etl = etl_header_gen.next(etl, name=a.name)
                    dest_etl.machine = machine_metadata
                    dest_etl.url = a.url

                    with TempFile() as tempfile:
                        Log.note("download {{url}}", url=a.url)
                        download_file(a.url, tempfile.abspath)
                        with open(tempfile.abspath, str("rb")) as fstream:
                            with Timer("process {{url}}", param={"url": a.url}):
                                destination.write_lines(
                                    dest_key,
                                    (
                                        value2json(normalize_property(source_key, Data(name=data.missing, value=Null), repo, dest_etl, i, please_stop))
                                        for i, data in enumerate(stream.parse(
                                            scompressed2ibytes(fstream),
                                            "missing",
                                            {"missing"}
                                        ))
                                    )
                                )

                    file_num += 1
                    output.append(dest_key)

        return output


def normalize_property(source_key, data, repo, parent_etl, i, please_stop):
    if please_stop:
        Log.error("Shutdown detected. Stopping early")

    etl = {
        "id": i,
        "source": parent_etl,
        "type": "join"
    }
    repo.changeset.description = None
    repo.branch = None

    value = {
        "_id":  etl2key(wrap(etl)),
        "file": {
            "full_name": data.name,
            "name": data.name.split('/')[-1],
            "type": extension(data.name),
            "path": path(data.name)
        },
        "bug": {
            "product": data.value[0],
            "component": data.value[1]
        },
        "repo": repo,
        "etl": etl
    }

    return value


# def normalize_missing(source_key, data, repo, parent_etl, i):
#     etl = {
#         "id": i,
#         "source": parent_etl,
#         "type": "join"
#     }
#     value = {
#         "_id":  etl2key(wrap(etl)),
#         "file": {"name": data.missing},
#         "revision": repo.changeset.id,
#         "etl": etl
#     }
#
#     return value


def path(name):
    return ['/'.join(p) for p in jx.prefixes(name.split('/'))]


def extension(filename):
     parts = filename.split('/')[-1].split('.')
     if len(parts) == 1:
         return None
     else:
         return parts[-1]

