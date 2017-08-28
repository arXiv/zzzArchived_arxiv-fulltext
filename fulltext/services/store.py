"""The request table tracks work on arXiv documents."""

from fulltext import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime
import os
from flask import _app_ctx_stack as stack

logger = logging.getLogger(__name__)


class FullTextStoreSession(object):
    table_name = 'FullText'

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, region_name: str, version: str) -> None:
        self.version = version
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)
        try:
            self._create_table()
        except ClientError as e:
            pass
        self.table = self.dynamodb.Table(self.table_name)

    def _create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {'AttributeName': 'document', 'KeyType': 'HASH'},
                {'AttributeName': 'created', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {"AttributeName": 'document', "AttributeType": "S"},
                {"AttributeName": 'created', "AttributeType": "S"}
            ],
            ProvisionedThroughput={    # TODO: make this configurable.
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        waiter = table.meta.client.get_waiter('table_exists')
        waiter.wait(TableName=self.table_name)

    def create(self, document_id: str, content: str) -> None:
        """
        Create a new extraction event entry.

        Parameters
        ----------
        sequence_id : int
        state : str
        document_id : str

        Raises
        ------
        IOError
        """
        entry = {
            'document': document_id,
            'version': self.version,
            'created':  datetime.now().isoformat(),
            'content': content
        }
        try:
            self.table.put_item(Item=entry)
        except ClientError as e:
            raise IOError('Failed to create: %s' % e) from e

    def latest(self, document_id: str) -> dict:
        """
        Retrieve the most recent extraction for a document.

        Parameters
        ----------
        document_id : int

        Returns
        -------
        dict
        """
        try:
            response = self.table.query(
                Limit=1,
                ScanIndexForward=False,
                KeyConditionExpression=Key('document').eq(document_id)
            )
        except ClientError as e:
            raise IOError('Could not connect to fulltext store: %s' % e)
        if len(response['Items']) == 0:
            return None
        return response['Items'][0]


class FullTextStore(object):
    """Fulltext store service integration."""

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('FULLTEXT_DYNAMODB_ENDPOINT', None)
        app.config.setdefault('AWS_ACCESS_KEY_ID', 'asdf1234')
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', 'fdsa5678')
        app.config.setdefault('FULLTEXT_AWS_REGION', 'us-east-1')
        app.config.setdefault('VERSION', 'none')

    def get_session(self) -> None:
        try:
            endpoint_url = self.app.config['FULLTEXT_DYNAMODB_ENDPOINT']
            aws_access_key = self.app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = self.app.config['AWS_SECRET_ACCESS_KEY']
            region_name = self.app.config['FULLTEXT_AWS_REGION']
            version = self.app.config['VERSION']
        except (RuntimeError, AttributeError) as e:    # No app context.
            endpoint_url = os.environ.get('FULLTEXT_DYNAMODB_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'asdf')
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'fdsa')
            region_name = os.environ.get('FULLTEXT_AWS_REGION', 'us-east-1')
            version = os.environ.get('VERSION', 'none')
        return FullTextStoreSession(endpoint_url, aws_access_key,
                                    aws_secret_key, region_name, version)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'fulltext_store'):
                ctx.fulltext_store = self.get_session()
            return ctx.fulltext_store
        return self.get_session()     # No application context.


store = FullTextStore()
