from typing import Tuple, Optional
from functools import wraps
from hashlib import md5
import boto3
import botocore
from flask import Flask
from arxiv.base.globals import get_application_global, get_application_config


class DoesNotExist(RuntimeError):
    """The requested fulltext content does not exist."""


class S3Session(object):
    """Represents a session with S3."""

    def __init__(self, bucket: str, version: str, verify: bool = False,
                 region_name: Optional[str] = None,
                 endpoint_url: Optional[str] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None) -> None:
        """Initialize with connection config parameters."""
        self.bucket = bucket
        self.version = version
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
            verify=verify,
            region_name=region_name
        )

    def store(self, paper_id: str, content: str, version: Optional[str] = None,
              content_format: str = 'plain') -> None:
        """Store fulltext content."""
        if version is None:
            version = self.version
        body = content.encode('utf-8')
        try:
            self.client.put_object(
                Body=body,
                Bucket=self.bucket,
                ContentHash=md5(body).hexdigest(),
                ContentType='text/plain',
                Key=f'{paper_id}/{version}/{content_format}',
            )
        except botocore.exceptions.ClientError as e:
            raise RuntimeError(f'Unhandled exception: {e}') from e

    def retrieve(self, paper_id: str, version: Optional[str] = None,
                 content_format: str = 'plain') -> None:
        """Retrieve fulltext content."""
        if version is None:
            version = self.version
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=f'{paper_id}/{version}/{content_format}'
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise DoesNotExist(f'No fulltext content for {paper_id} with '
                                   f'extractor version {version} in format '
                                   f'{content_format}') from e
            raise RuntimeError(f'Unhandled exception: {e}') from e
        return response['Body'].decode('utf-8')

    def exists(self, paper_id: str, version: Optional[str] = None,
               content_format: str = 'plain') -> None:
        """Check whether fulltext content exists."""
        if version is None:
            version = self.version
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=f'{paper_id}/{version}/{content_format}'
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise RuntimeError(f'Unhandled exception: {e}') from e
        return True


@wraps(S3Session.store)
def store(paper_id: str, content: str, version: Optional[str] = None,
          content_format: str = 'plain') -> None:
    """Store fulltext content using the current S3 session."""
    return current_session().store(paper_id, content, version, content_format)


@wraps(S3Session.retrieve)
def retrieve(paper_id: str, version: Optional[str] = None,
             content_format: str = 'plain') -> None:
    """Retrieve fulltext content using the current S3 session."""
    return current_session().retrieve(paper_id, version, content_format)


@wraps(S3Session.exists)
def exists(paper_id: str, version: Optional[str] = None,
           content_format: str = 'plain') -> None:
    """Check if fulltext content exists using the current S3 session."""
    return current_session().exists(paper_id, version, content_format)


def init_app(app: Flask) -> None:
    """Set defaults for required configuration parameters."""
    app.config.setdefault('AWS_REGION', 'us-east-1')
    app.config.setdefault('AWS_ACCESS_KEY_ID', None)
    app.config.setdefault('AWS_SECRET_ACCESS_KEY', None)
    app.config.setdefault('S3_ENDPOINT', None)
    app.config.setdefault('S3_VERIFY', True)
    app.config.setdefault('S3_BUCKET', 'arxiv-fulltext')
    app.config.setdefault('VERSION', "0.0")


def get_session() -> S3Session:
    """Create a new :class:`botocore.client.S3` session."""
    config = get_application_config()
    access_key = config.get('AWS_ACCESS_KEY_ID')
    secret_key = config.get('AWS_SECRET_ACCESS_KEY')
    endpoint = config.get('S3_ENDPOINT')
    verify = config.get('S3_VERIFY')
    region = config.get('AWS_REGION')
    bucket = config.get('S3_BUCKET')
    version = config.get('VERSION')
    return S3Session(bucket, version, verify, region,
                     endpoint, access_key, secret_key)


def current_session() -> S3Session:
    """Get the current new :class:`botocore.client.S3` for this application."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'store' not in g:
        g.store = get_session()
    return g.store
