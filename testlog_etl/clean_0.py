from pyLibrary import convert
from pyLibrary.aws.s3 import Bucket
from pyLibrary.debugs import startup
from pyLibrary.debugs.logs import Log


def main():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):

            Bucket(settings.destination).get_key("0").write(
                convert.value2json({"action": "shutdown", "timestamp": 1420056396165, "next_key": 10000, "source_key": 1000000})
            )

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()


