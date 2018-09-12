import gzip

from mo_dots import Null
from mo_files import File


class Destination(object):

    def __init__(self, filename):
        try:
            File(filename).parent.create()
        except Exception as e:
            pass
        self.filename = filename
        self.count = 0

    def extend(self, lines):
        self.write_lines(Null, lines)

    def write_lines(self, key, lines):
        with gzip.GzipFile(File(self.filename).abspath, mode='w') as archive:
            for l in lines:
                archive.write(l.encode("utf8"))
                archive.write(b"\n")
                self.count += 1


