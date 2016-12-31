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

from pyDots import Data


class Revision(Data):

    def __hash__(self):
        return hash((self.branch.name.lower(), self.changeset.id[:12]))

    def __eq__(self, other):
        if other==None:
            return False
        return (self.branch.name.lower(), self.changeset.id[:12]) == (other.branch.name.lower(), other.changeset.id[:12])

