import gzip

from mo_dots import Null
from mo_files import File
from mo_future import __builtin__
from mo_json import value2json


class Destination(object):
    def __init__(self, filename):
        self.filename = filename
        self.file = File(filename)
        try:
            self.file.parent.create()
        except Exception as e:
            pass
        if self.file.extension != "gzip":
            self.file = self.file.add_extension("gzip")
        self.count = 0

    def extend(self, lines):
        self.write_lines(Null, map(value2json, lines))

    def write_lines(self, key, lines):
        with gzip.GzipFile(
            self.filename, fileobj=__builtin__.open(self.file.abspath, "wb"), mode="w"
        ) as archive:
            for l in lines:
                archive.write(l.encode("utf8"))
                archive.write(b"\n")
                self.count += 1
