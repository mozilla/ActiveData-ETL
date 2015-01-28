from pyLibrary import aws, strings
from pyLibrary.aws.s3 import Connection
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import Q
from pyLibrary.times.dates import Date
from testlog_etl import key2etl, etl2path


def main():
    try:
        settings = startup.read_settings(defs=[
            {
                "name": ["--bucket"],
                "help": "bucket to reprocess",
                "type": str,
                "dest": "bucket",
                "required": True
            },
            {
                "name": ["--begin", "--start"],
                "help": "lowest key (or prefix) to reprocess",
                "type": str,
                "dest": "start",
                "default": "1",
                "required": False
            },
            {
                "name": ["--end", "--stop"],
                "help": "highest key (or prefix) to reprocess",
                "type": str,
                "dest": "end",
                "default": None,
                "required": False
            }
        ])
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            with aws.Queue(settings.work_queue) as work_queue:
                source = Connection(settings.aws).get_bucket(settings.args.bucket)

                prefix = strings.common_prefix(settings.args.start, settings.args.end)
                start = Version(settings.args.start)
                end = Version(settings.args.end)

                all_keys = source.keys(prefix=prefix)
                for k in Q.sort(all_keys):
                    p = Version(k)
                    if start <= p < end:
                        Log.note("Adding {{key}}", {"key":k})
                        now = Date.now()
                        work_queue.add({
                            "bucket": settings.args.bucket,
                            "key": k,
                            "timestamp":now.milli,
                           "date/time":now.format()
                        })

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


class Version(object):
    """
    BOX A VERSION NUMBER SO IT CAN BE COMPARED USING >, >=, ==, !=, <=, < OPERATORS
    """


    def __init__(self, key):
        if key == None:
            self.path=[]
            return
        etl = key2etl(key)
        self.path = etl2path(etl)


    def __lt__(self, other):
        if not self.path or not other.path:
            return True
        return comparePath(self.path, other.path) == 1

    def __le__(self, other):
        if not self.path or not other.path:
            return True
        return comparePath(self.path, other.path) >= 0

    def __gt__(self, other):
        if not self.path or not other.path:
            return True
        return comparePath(self.path, other.path) == -1

    def __ge__(self, other):
        if not self.path or not other.path:
            return True
        return comparePath(self.path, other.path) <= 0


def comparePath(a, b):
    # ASSUME a AND b ARE VERSION NUMBERS, RETURN THE COMPARISON
    # a < b  == 1
    # a > b  == -1
    e = 0
    for i in range(min(len(a), len(b))):
        if a[i] != b[i]:
            e = i
            break
    else:
        if len(a) < len(b):
            return 1
        if len(a) > len(b):
            return -1
        return 0

    if a[e] < b[e]:
        return 1
    if a[e] > b[e]:
        return -1
    return 0







if __name__ == "__main__":
    main()


