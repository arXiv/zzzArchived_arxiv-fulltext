import boto3
from botocore.exceptions import ClientError
import os

if __name__ == '__main__':
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    endpoint = os.environ.get('KINESIS_ENDPOINT')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    verify = os.environ.get('KINESIS_VERIFY') == 'true'
    stream_name = os.environ.get('STREAM_NAME', 'PDFIsAvailable')

    client = boto3.client('kinesis', region_name=region, endpoint_url=endpoint,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key,
                          verify=verify)
    try:
        client.describe_stream(StreamName='PDFIsAvailable')
    except ClientError:
        client.create_stream(StreamName='PDFIsAvailable', ShardCount=1)
