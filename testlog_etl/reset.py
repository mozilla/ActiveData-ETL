from pyLibrary import aws
from pyLibrary.aws.s3 import Connection
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from testlog_etl import key2etl


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

                start = Rev(settings.args.start)
                end = Rev(settings.args.end)

                for k in source.keys():
                    p = Rev(k)
                    if start <= p < end:
                        work_queue.add({
                            "bucket": settings.args.bucket,
                            "key": k
                        })

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


class Rev(object):
    def __init__(self, key):
        if key == None:
            self.path=[]
            return
        etl = key2etl(key)
        self.path = []
        while etl:
            self.path.insert(0, etl.id)
            while etl.type and etl.type != "join":
                etl = etl.source
            etl = etl.source

    def __lt__(self, other):
        if not self.path or not other.path:
            return True
        return comparePath(self.path, other.path) == -1

    def __le__(self, other):
        if not self.path or not other.path:
            return True
        return comparePath(self.path, other.path) <= 0

    def __gt__(self, other):
        if not self.path or not other.path:
            return True
        return comparePath(self.path, other.path) == 1

    def __ge__(self, other):
        if not self.path or not other.path:
            return True
        return comparePath(self.path, other.path) >= 0


def comparePath(a, b):
    # ASSUME a AND b ARE VERSION NUMBERS, RETURN THE COMPARISON
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


