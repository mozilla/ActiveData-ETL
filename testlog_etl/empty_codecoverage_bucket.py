from boto.s3.connection import S3Connection, Bucket

"""
This script will delete everything in the active-data-codecoverage-dev bucket
"""

conn = S3Connection("", "")

b = Bucket(conn, "active-data-codecoverage-dev")
for x in b.list():
    print("Deleting " + x.key)
    b.delete_key(x.key)
