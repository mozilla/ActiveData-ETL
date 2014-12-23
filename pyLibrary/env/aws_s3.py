import boto
from pyLibrary import convert

from pyLibrary.debugs.logs import Log
from pyLibrary.structs import nvl


class File(object):
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    def read(self):
        return self.bucket.read(self.key)

    def write(self, value):
        self.bucket.write(self.key, value)



class Bucket(object):
    def __init__(self, settings, public=False):
        """
        SETTINGS:
        bucket - NAME OF THE BUCKET
        aws_access_key_id - CREDENTIAL
        aws_secret_access_key - CREDENTIAL
        """
        self.settings = settings
        self.settings.public = nvl(self.settings.public, public)
        self.connection = None
        self.bucket = None


    def __enter__(self):
        try:
            if self.connection:
                Log.error("Already connected")

            aws_access_key_id=nvl(
                self.settings.aws_access_key_id,
                self.settings.access_key_id,
                self.settings.username,
                self.settings.user
            )
            aws_secret_access_key=nvl(
                self.settings.aws_secret_access_key,
                self.settings.secret_access_key,
                self.settings.password
            )
            if aws_access_key_id == None or aws_secret_access_key == None:
                Log.error("require aws_access_key_id and aws_secret_access_key to connect to S3")

            self.connection = boto.connect_s3(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )

            self.bucket = self.connection.get_bucket(self.settings.bucket, validate=False)
            return self
        except Exception, e:
            Log.error("Problem connecting to {{bucket}}", {"bucket": self.settings.bucket}, e)


    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def get_key(self, key):
        return File(self, key)

    def read(self, key):
        try:
            value = self.bucket.get_key(key)
        except Exception, e:
            Log.error("S3 read error", e)

        if value == None:
            Log.error("{{key}} does not exist", {"key": key})

        try:
            json = value.get_contents_as_string()
        except Exception, e:
            Log.error("S3 read error", e)

        if json == None:
            return None
        return convert.utf82unicode(json)



    def write(self, key, value):
        try:
            key = self.bucket.new_key(key)
            key.set_contents_from_string(convert.unicode2utf8(value))
            if self.settings.public:
                key.set_acl('public-read')
        except Exception, e:
            Log.error("S3 write error", e)


    @property
    def name(self):
        return self.settings.bucket
