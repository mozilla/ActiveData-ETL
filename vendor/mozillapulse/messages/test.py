# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from base import GenericMessage

# ------------------------------------------------------------------------------
# Simple test message
# ------------------------------------------------------------------------------

class TestMessage(GenericMessage):

    def __init__(self):
        super(TestMessage, self).__init__()
        self.routing_parts.append('test')