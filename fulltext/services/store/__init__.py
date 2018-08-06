"""
Content store for extracted full text.

Uses S3 as the underlying storage facility.
"""
import json
from typing import Tuple, Optional, Dict, Union
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

    def __init__(self, buckets: str, version: str, verify: bool = False,
                 region_name: Optional[str] = None,
                 endpoint_url: Optional[str] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None) -> None:
        """Initialize with connection config parameters."""
        self.buckets = buckets
        self.version = version
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        # Only add credentials to the client if they are explicitly set.
        # If they are not set, boto3 falls back to environment variables and
        # credentials files.
        params = dict(region_name=region_name)
        if aws_access_key_id and aws_secret_access_key:
            params.update(dict(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            ))
        if endpoint_url:
            params.update(dict(
                endpoint_url=endpoint_url,
                verify=verify
            ))
        self.client = boto3.client('s3', **params)

    def _create_placeholder(self, data: dict) -> bytes:
        return f'PLACEHOLDER:::{json.dumps(data)}'.encode('utf-8')

    def _parse_placeholder(self, raw: bytes) -> dict:
        return json.loads(raw.decode('utf-8').split(':::', 1)[1])

    def _is_placeholder(self, raw: bytes) -> bool:
        return raw.decode('utf-8').startswith('PLACEHOLDER:::')

    def store(self, paper_id: str, content: Union[str, dict],
              version: Optional[str] = None, content_format: str = 'plain',
              bucket: str = 'arxiv', is_placeholder: bool = False) -> None:
        """
        Store fulltext content.

        Parameters
        ----------
        paper_id : str
            The unique identifier for the paper to which this content
            corresponds. This will usually be an arXiv ID, but could also be
            a submission or other ID.
        content : str or dict
            The text content to store, or placeholder data.
        version : str or None
            The version of the extractor used to generate this content. If
            ``None``, the current extractor version will be used.
        content_format : str
            Should be ``'plain'`` or ``'psv'``.
        bucket : str
            Default is ``'arxiv'``. Used in conjunction with :attr:`.buckets`
            to determine the S3 bucket where this content should be stored.
        is_placeholder : bool
            If ``True``, ``content`` should be a JSON-serializable ``dict``.

        """
        if version is None:
            version = self.version
        if is_placeholder:
            body = self._create_placeholder(content)
        else:
            body = content.encode('utf-8')
        try:
            self.client.put_object(
                Body=body,
                Bucket=self._get_bucket(bucket),
                ContentMD5=md5(body).hexdigest(),
                ContentType='text/plain',
                Key=f'{paper_id}/{version}/{content_format}',
            )
        except botocore.exceptions.ClientError as e:
            raise RuntimeError(f'Unhandled exception: {e}') from e

    def retrieve(self, paper_id: str, version: Optional[str] = None,
                 content_format: str = 'plain', bucket: str = 'arxiv') -> Dict:
        """
        Retrieve fulltext content.

        Parameters
        ----------
        paper_id : str
            The unique identifier for the paper to which this content
            corresponds. This will usually be an arXiv ID, but could also be
            a submission or other ID.
        version : str or None
            The version of the extractor for which content should be retrieved.
            If ``None``, the current extractor version will be used.
        content_format : str
            The format to retrieve. Should be ``'plain'`` or ``'psv'``.
        bucket : str
            Default is ``'arxiv'``. Used in conjunction with :attr:`.buckets`
            to determine the S3 bucket from which the content should be
            retrieved

        Returns
        -------
        dict

        """
        if version is None:
            version = self.version
        try:
            response = self.client.get_object(
                Bucket=self._get_bucket(bucket),
                Key=f'{paper_id}/{version}/{content_format}'
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                raise DoesNotExist(f'No fulltext content for {paper_id} with '
                                   f'extractor version {version} in format '
                                   f'{content_format}') from e
            raise RuntimeError(f'Unhandled exception: {e}') from e
        content = response['Body'].read()
        if self._is_placeholder(content):
            return {'placeholder': self._parse_placeholder(content)}
        return {
            'content': content.decode('utf-8'),
            'version': version,
            'format': content_format,
            'etag': response['ETag'][1:-1],
            'created': response['LastModified']
        }

    # TODO: consider returning metadata from the HEAD request, instead of just
    # a bool. If it's useful?
    def exists(self, paper_id: str, version: Optional[str] = None,
               content_format: str = 'plain', bucket: str = 'arxiv') -> bool:
        """
        Check whether fulltext content (or a placeholder) exists.

        Parameters
        ----------
        paper_id : str
            The unique identifier for the paper to which this content
            corresponds. This will usually be an arXiv ID, but could also be
            a submission or other ID.
        version : str or None
            The version of the extractor for which content should be retrieved.
            If ``None``, the current extractor version will be used.
        content_format : str
            The format to retrieve. Should be ``'plain'`` or ``'psv'``.
        bucket : str
            Default is ``'arxiv'``. Used in conjunction with :attr:`.buckets`
            to determine the S3 bucket from which the content should be
            retrieved

        Returns
        -------
        bool
        """
        if version is None:
            version = self.version

        try:
            self.client.head_object(
                Bucket=self._get_bucket(bucket),
                Key=f'{paper_id}/{version}/{content_format}'
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise RuntimeError(f'Unhandled exception: {e}') from e
        return True

    def create_bucket(self):
        """Create S3 buckets. This is just for testing."""
        for key, bucket in self.buckets:
            self.client.create_bucket(Bucket=bucket)

    def _get_bucket(self, bucket: str) -> str:
        try:
            name: str = dict(self.buckets)[bucket]
        except KeyError as e:
            raise RuntimeError(f'No such bucket: {bucket}') from e
        return name


@wraps(S3Session.store)
def store(paper_id: str, content: Union[str, dict],
          version: Optional[str] = None, content_format: str = 'plain',
          bucket: str = 'arxiv', is_placeholder: bool = False) -> None:
    """Store fulltext content using the current S3 session."""
    s = current_session()
    return s.store(paper_id, content, version, content_format, bucket,
                   is_placeholder)


@wraps(S3Session.retrieve)
def retrieve(paper_id: str, version: Optional[str] = None,
             content_format: str = 'plain', bucket: str = 'arxiv') -> None:
    """Retrieve fulltext content using the current S3 session."""
    s = current_session()
    return s.retrieve(paper_id, version, content_format, bucket)


@wraps(S3Session.exists)
def exists(paper_id: str, version: Optional[str] = None,
           content_format: str = 'plain', bucket: str = 'arxiv') -> None:
    """Check if fulltext content exists using the current S3 session."""
    s = current_session()
    return s.exists(paper_id, version, content_format, bucket)


def init_app(app: Flask) -> None:
    """Set defaults for required configuration parameters."""
    app.config.setdefault('AWS_REGION', 'us-east-1')
    app.config.setdefault('AWS_ACCESS_KEY_ID', None)
    app.config.setdefault('AWS_SECRET_ACCESS_KEY', None)
    app.config.setdefault('S3_ENDPOINT', None)
    app.config.setdefault('S3_VERIFY', True)
    app.config.setdefault('S3_BUCKET', [])
    app.config.setdefault('VERSION', "0.0")


def get_session() -> S3Session:
    """Create a new :class:`botocore.client.S3` session."""
    config = get_application_config()
    access_key = config.get('AWS_ACCESS_KEY_ID')
    secret_key = config.get('AWS_SECRET_ACCESS_KEY')
    endpoint = config.get('S3_ENDPOINT')
    verify = config.get('S3_VERIFY')
    region = config.get('AWS_REGION')
    buckets = config.get('S3_BUCKETS')
    version = config.get('VERSION')
    return S3Session(buckets, version, verify, region,
                     endpoint, access_key, secret_key)


def current_session() -> S3Session:
    """Get the current new :class:`botocore.client.S3` for this application."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'store' not in g:
        g.store = get_session()
    return g.store
