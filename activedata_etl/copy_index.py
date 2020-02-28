# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import, division, unicode_literals

from jx_elasticsearch import elasticsearch
from mo_logs import Log, startup, constants


def copy_index(config):
    # COPY THE INDEX METADATA
    cluster = elasticsearch.Cluster(config.source)

    source = cluster.get_index(config.source)
    source_config = source.cluster.get_metadata().indices[source.settings.index]

    dest_config = config.destination | config.source
    if dest_config.schema == None:
        # USE THE SOURCE SCHEMA, IF NOT DECLARED
        dest_config.schema.settings.index.number_of_replicas = source_config.settings.index.number_of_replicas
        dest_config.schema.settings.index.number_of_shards = source_config.settings.index.number_of_shards
        dest_config.schema.mappings = source_config.mappings

    if dest_config.index == source.settings.index:
        Log.error("not expecting index to be the same")

    dest = cluster.get_or_create_index(read_only=False, kwargs=dest_config)

    cluster.post("_reindex?wait_for_completion=false", json={
        "conflicts": "proceed",
        "source": {
            "index": source.settings.index
        },
        "dest": {
            "index": dest.settings.index,
            "version_type": "internal"
        }
    })


def main():
    try:
        config = startup.read_settings(defs=[
            {
                "name": ["--from", "--source"],
                "help": "name of the source index",
                "type": str,
                "dest": "source",
                "required": False
            },
            {
                "name": ["--to", "--destination", "--dest"],
                "help": "name of the destination index",
                "type": str,
                "dest": "destination",
                "required": False
            }
        ])
        constants.set(config.constants)
        Log.start(config.debug)

        config.source = {"index": config.args.source} | config.source
        config.destination = {"index": config.args.destination} | config.destination

        copy_index(config)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__=="__main__":
    main()
