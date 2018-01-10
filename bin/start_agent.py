"""Creates the PDFIsAvailable stream if it does not already exist."""
import boto3
from botocore.exceptions import ClientError
import os
from fulltext.services import credentials

if __name__ == '__main__':
    access_key, secret, token = credentials.get_credentials()
    endpoint = os.environ.get('KINESIS_ENDPOINT')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    verify = os.environ.get('KINESIS_VERIFY') == 'true'
    stream_name = os.environ.get('STREAM_NAME', 'PDFIsAvailable')

    client = boto3.client('kinesis', region_name=region, endpoint_url=endpoint,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret,
                          aws_session_token=token,
                          verify=verify)
    try:
        client.describe_stream(StreamName='PDFIsAvailable')
    except ClientError:
        client.create_stream(StreamName='PDFIsAvailable', ShardCount=1)
