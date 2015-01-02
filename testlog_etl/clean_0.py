from pyLibrary import convert
from pyLibrary.aws.s3 import Bucket
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log
from pyLibrary.queries import Q


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
            source = Bucket(settings.source)
            destination = Bucket(settings.destination)
            new_key = 9999
            for k in Q.reverse(Q.sort(source.keys())):
                nk = unicode(new_key)+":"+unicode(new_key*100)
                value = source.read(k)
                destination.write(nk, value)
                Log.note("done {{from}} -> {{to}}", {"from":k, "to":nk})
                new_key -= 1

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


