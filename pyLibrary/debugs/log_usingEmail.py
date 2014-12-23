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

from .logs import BaseLog
from pyLibrary.env.emailer import Emailer
from pyLibrary.strings import expand_template


class Log_usingEmail(BaseLog):

    def __init__(self, settings):
        """
        SEND WARNINGS AND ERRORS VIA EMAIL

        settings = {
            "type":"email",
            "from_address": "klahnakoski@mozilla.com",
            "to_address": "klahnakoski@mozilla.com",
            "subject": "Problem in Pulse Logger",
            "host": "mail.mozilla.com",
            "port": 465,
            "username": "username",
            "password": "password",
            "use_ssl": 1
        }

        """
        assert settings.log_type == "email", "Expecing settings to be of type 'email'"
        self.settings = settings

    def write(self, template, params):
        if params.params.warning.template or params.params.warning.template:
            try:
                with Emailer(self.settings.emailer) as emailer:
                    emailer.send_email(
                        from_address=self.settings.from_address,
                        to_addrs=self.settings.to_address,
                        subject=self.settings.subject,
                        text_data=expand_template(template, params)
                    )
            except Exception, e:
                pass  # ALL HOPE IS LOST



