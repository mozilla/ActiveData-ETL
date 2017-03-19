# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Trung Do (chin.bimbo@gmail.com)
#
from __future__ import division
from __future__ import unicode_literals


def minimize_task(task):
    """
    task objects are a little large, scrub them of some of the
    nested arrays
    :param task: task cluster normalized object
    :return: altered object
    """

    task.etl = None
    task.action.timings = None
    task.action.etl = None
    task.repo.changeset.files = None
    task.task.artifacts = None
    task.task.runs = None
    task.task.env = None
    task.task.scopes = None
    task.task.routes = None
    task.task.command = None
    task.task.tags = None
    task.build.build = None
    task.build.task = {"id": task.build.task.id}
