# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division


import subprocess

try:
    from _subprocess import CREATE_NEW_PROCESS_GROUP, CREATE_NEW_CONSOLE

    flags = CREATE_NEW_PROCESS_GROUP
except Exception, e:
    flags = None



def get_git_revision():
    """
    GET THE CURRENT GIT REVISION
    """
    proc = subprocess.Popen(
        ["git", "log", "-1"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1,
        creationflags=flags
    )

    try:
        while True:
            line = proc.stdout.readline().strip()
            if not line:
                continue
            if line.startswith("commit "):
                return line[7:]
    finally:
        try:
            proc.wait()
        except Exception:
            pass

