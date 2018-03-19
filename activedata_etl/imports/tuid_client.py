
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

from mo_logs import Log
from mo_times import DAY, Timer
from pyLibrary.env import http
from pyLibrary.meta import cache

DEBUG = True

class TuidClient(object):

    def __init__(self, tuid_endpoint):
        self.enabled = True
        self.tuid_endpoint = tuid_endpoint

    def annotate_source(self, revision, source):
        """
        :param revision: revision number to use for lookup
        :param source: coverage that will be marked up with tuids
        :return:
        """
        if not self.enabled:
            return

        line_to_tuid = self.get_tuid(revision, source.file.name)
        if line_to_tuid is not None:
            source.file.tuid_covered = [
                {"line": line, "tuid": line_to_tuid[line]}
                for line in source.file.covered
                if line_to_tuid[line]
            ]
            source.file.tuid_uncovered = [
                {"line": line, "tuid": line_to_tuid[line]}
                for line in source.file.uncovered
                if line_to_tuid[line]
            ]

    @cache(duration=DAY, lock=True)
    def get_tuid(self, revision, file):
        if not self.enabled:
            return None

        with Timer(
            "ask tuid service for {{file}} at {{revision|left(12)}}",
            {"file": file, "revision": revision},
            debug=DEBUG
        ):
            try:
                response = http.post_json(
                    self.tuid_endpoint,
                    json={
                        "from": "files",
                        "where": {"and": [
                            {"eq": {"revision": revision}},
                            {"eq": {"path": file}}
                        ]}
                    },
                    timeout=30
                )
                return {h: v for h, v in zip(response.header, response.data[0])}['tuids']
            except Exception as e:
                self.enabled = False
                Log.warning("TUID service has problems, disabling.", cause=e)
                return None

