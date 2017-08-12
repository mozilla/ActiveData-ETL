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
from multiprocessing import Process, Queue


def say_hello(queue):
    print "Hello, %s" % queue.get()


if __name__ == '__main__':
    queue = Queue()

    p = Process(target=say_hello, args=(queue,))
    p.start()

    queue.put("Kyle")

    p.join()



