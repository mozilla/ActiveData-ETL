from pyLibrary import convert
from pyLibrary.dot import coalesce, wrap, unwrap
from pyLibrary.env import http
from pyLibrary.meta import cache, use_settings


class TreeHerder(object):
    @use_settings
    def __init__(self, timeout=None, settings=None):
        self.settings = settings

    @cache
    def get_branches(self):
        response = http.get(self.settings.branches.url, timeout=coalesce(self.settings.timeout, 30))
        branches = convert.json2value(convert.utf82unicode(response.content))
        return wrap({branch.name: unwrap(branch) for branch in branches})
