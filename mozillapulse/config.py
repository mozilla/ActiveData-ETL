# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

DEFAULT_PORT = 5672
DEFAULT_SSL_PORT = 5671

class PulseConfiguration:

    # Default values for Mozilla pulse
    defaults = {
        # Connection defaults
        'user': 'public',
        'password': 'public',
        'host': 'pulse.mozilla.org',
        'vhost': '/',
        'ssl': True,
        # Message defaults
        'serializer': 'json',
        'broker_timezone': 'US/Pacific',
        # Special vars which may be modified depending on other vars above.
        'port': DEFAULT_SSL_PORT,
    }

    def __init__(self, **kwargs):
        # Set any variables passed in.
        for key in kwargs:
            setattr(self, key, kwargs[key])

        # Set defaults for anything that isn't passed in.
        for key in self.defaults:
            if not hasattr(self, key):
                setattr(self, key, self.defaults[key])

        # Set defaults for special variables.
        if 'port' not in kwargs:
            if not self.ssl:
                self.port = DEFAULT_PORT

    @classmethod
    def read_from_config(self, cfg, section='pulse'):
        """Generates a PulseConfiguration object from a ConfigParser section.
        Any options not present in 'section' will be set to the standard
        default.  If 'section' does not exist at all, the returned
        PulseConfiguration will consist entirely of the defaults.
        """
        opts = {}
        if cfg.has_section(section):
            for opt_name, opt_dflt in PulseConfiguration.defaults.iteritems():
                if cfg.has_option(section, opt_name):
                    if isinstance(opt_dflt, basestring):
                        opts[opt_name] = cfg.get(section, opt_name)
                    elif isinstance(opt_dflt, bool):
                        opts[opt_name] = cfg.getboolean(section, opt_name)
                    elif isinstance(opt_dflt, int):
                        opts[opt_name] = cfg.getint(section, opt_name)
                    else:
                        raise ValueError  # just in case
        return PulseConfiguration(**opts)
