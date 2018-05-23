from mo_logs import Log

from mo_testing.fuzzytestcase import FuzzyTestCase

import mo_json_config
from pyLibrary.aws import s3


class TestListBucket(FuzzyTestCase):

    def test_list(self):
        settings = mo_json_config.get("file://tests/resources/fx-test-activedata.json")
        bucket = s3.Bucket(kwargs=settings).bucket
        for i in bucket.list():
            Log.note("{{name}}", name=i.key)


