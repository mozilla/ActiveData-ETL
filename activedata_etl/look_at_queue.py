# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import division
from __future__ import unicode_literals

from mo_logs import Log
from mo_logs import startup
from pyLibrary import aws


def list_queue(settings, num):
    queue = aws.Queue(settings)
    for i in range(num):
        content = queue.pop()
        Log.note("\n{{content|json}}", content=content)
    queue.rollback()


def scrub_queue(settings):
    queue = aws.Queue(settings)

    existing = set()

    for i in range(120000):
        content = queue.pop()
        try:
            if (content.key, content.bucket) not in existing:
                existing.add((content.key, content.bucket))
                queue.add(content)
                Log.note("KEEP {{content|json}}", content=content)
            else:
                Log.note("remove {{content|json}}", content=content)
        finally:
            queue.commit()


def main():
    try:
        settings = startup.read_settings(defs={
            "name": ["--num"],
            "help": "number to show",
            "type": int,
            "dest": "num",
            "default": '10',
            "required": False
        })
        Log.start(settings.debug)
        # scrub_queue(settings.source)
        list_queue(settings.source, settings.args.num)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
