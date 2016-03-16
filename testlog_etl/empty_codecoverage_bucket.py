import boto3 as boto3

"""
This script will delete everything in the active-data-codecoverage-dev bucket
"""

s3 = boto3.resource('s3')
objects_to_delete = s3.meta.client.list_objects(Bucket="active-data-codecoverage-dev")
delete_keys = {'Objects': [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]}
s3.meta.client.delete_objects(Bucket="active-data-codecoverage-dev", Delete=delete_keys)
