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

from pyLibrary import convert
from pyLibrary.aws import s3
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.maths.randoms import Random
from pyLibrary.queries import jx
from pyLibrary.thread.threads import Queue, Thread

known_tasks = set()
queue = Queue("packer")

START = 227400
RANDOM = Random.int(1000)

def compact(file):
    output = []
    lines = list(file.read_lines())
    for l in lines:
        data = convert.json2value(l)
        taskid = data.status.taskId
        if taskid in known_tasks:
            continue
        known_tasks.add(taskid)
        output.append(l)

    queue.add((output, file))
    Log.note("{{key}} file reduced from {{frum}} lines {{to}}", key=file.key, frum=len(lines), to=len(output))


def writer(bucket, please_stop):
    g = 0
    acc = []
    files = []
    for output, file in queue:
        acc.extend(output)
        files.append(file)
        if len(acc) >= 1000:
            key_num = START - g
            key = unicode(key_num) + ":" + unicode(int(key_num / 10) * 1000 + RANDOM)
            Log.note("Write new file {{file}}", file=key)
            bucket.write_lines(key, acc)
            for f in files:
                if f.key != key:
                    f.delete()
            acc = []
            files = []
            g += 1

def loop_all(bucket, please_stop):
    try:
        all_keys = jx.reverse(jx.sort(set(map(int, bucket.keys(delimiter=":")))))
        for k in all_keys:
            if please_stop:
                return
            if k > START:
                continue
            try:
                compact(bucket.get_key(unicode(k)))
            except Exception, e:
                Log.warning("could not process", cause=e)
    finally:
        queue.add(Thread.STOP)


def main():
    try:
        settings = startup.read_settings()
        bucket = s3.Bucket(settings=settings.source)

        Log.alert(" BE SURE TO \"exit\", OTHERWISE YOU WILL HAVE DATA LOSS")
        Thread.run("loop", loop_all, bucket)
        Thread.run("bucket writer thread", writer, bucket)

        Thread.wait_for_shutdown_signal(allow_exit=True)

    except Exception, e:
        Log.error("Problem with compaction", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
