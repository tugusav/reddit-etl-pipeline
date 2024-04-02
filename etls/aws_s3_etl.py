import boto3
import logging
import sys
import os
from botocore.exceptions import ClientError
from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def connect_to_s3(): 
    try:
        s3 = boto3.client('s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        logging.info("Connected to S3")
    except Exception as e:
        logging.error(e)
        sys.exit(1)
    return s3

def create_bucket_if_not_exists(s3: boto3.client, bucket_name: str, region: str):
    try:
        # check if bucket exists
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        if bucket_name not in buckets:
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': region})
            logging.info(f"Bucket {bucket_name} created")
        else:
            logging.info(f"Bucket {bucket_name} already exists")
    except Exception as e:
        logging.error(e)
        sys.exit(1)

def upload_file(s3: boto3.client, file_name, bucket, object_name=None):

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = s3.upload_file(file_name, bucket, 'raw/' + object_name)
        print('File uploaded to S3 bucket')
    except ClientError as e:
        logging.error(e)
        print('File not uploaded')
        return False
    return True