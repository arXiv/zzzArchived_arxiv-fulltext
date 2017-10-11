"""The request table tracks work on arXiv documents."""

from fulltext import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime
import os
from flask import _app_ctx_stack as stack
import gzip
logger = logging.getLogger(__name__)


class FullTextStoreSession(object):
    table_name = 'FullText'

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, aws_session_token: str,
                 region_name: str, verify: bool=True,
                 version: str="0.0") -> None:
        logger.debug('New session with dynamodb at %s' % endpoint_url)
        self.version = version
        self.dynamodb = boto3.resource('dynamodb', verify=verify,
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key,
                                       aws_session_token=aws_session_token)
        logger.debug('New dynamodb resource: %s' % str(id(self.dynamodb)))
        try:
            self._create_table()
            logger.debug('Created table: %s' % self.table_name)
        except ClientError as e:
            logger.debug('Table already exists: %s' % self.table_name)
            pass
        self.table = self.dynamodb.Table(self.table_name)
        logger.debug('New table object: %s' % str(id(self.table)))

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
                'ReadCapacityUnits': 500,
                'WriteCapacityUnits': 500
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
        logger.debug('Store record for %s' % document_id)
        logger.debug(content)
        entry = {
            'document': document_id,
            'version': self.version,
            'created':  datetime.now().isoformat(),
            'content': gzip.compress(bytes(content, encoding='utf-8'))
        }
        try:
            self.table.put_item(Item=entry)
            logger.debug('Store record for %s successful' % document_id)
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
        logger.debug('Getting latest record for %s' % document_id)
        try:
            response = self.table.query(
                Limit=1,
                ScanIndexForward=False,
                KeyConditionExpression=Key('document').eq(document_id)
            )
            logger.debug('Got response with %i results' %
                         len(response['Items']))
        except ClientError as e:
            raise IOError('Could not connect to fulltext store: %s' % e)
        if len(response['Items']) == 0:
            return None
        item = response['Items'][0]
        return {
            'document': item['document'],
            'version': item.get('version'),
            'created': item['created'],
            'content': gzip.decompress(item['content'].value).decode('utf-8')
        }



class FullTextStore(object):
    """Fulltext store service integration."""

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('DYNAMODB_ENDPOINT', None)
        app.config.setdefault('AWS_REGION', 'us-east-1')
        app.config.setdefault('VERSION', 'none')
        app.config.setdefault('DYNAMODB_VERIFY', 'true')

    def get_session(self) -> None:
        try:
            endpoint_url = self.app.config['DYNAMODB_ENDPOINT']
            aws_access_key = self.app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = self.app.config['AWS_SECRET_ACCESS_KEY']
            region_name = self.app.config['AWS_REGION']
            version = self.app.config['VERSION']
            aws_session_token = self.app.config['AWS_SESSION_TOKEN']
            verify = self.app.config['DYNAMODB_VERIFY'] == 'true'
        except (RuntimeError, AttributeError) as e:    # No app context.
            endpoint_url = os.environ.get('DYNAMODB_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'asdf')
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'fdsa')
            region_name = os.environ.get('AWS_REGION', 'us-east-1')
            version = os.environ.get('VERSION', '0.0')
            aws_session_token = os.environ.get('AWS_SESSION_TOKEN', None)
            verify = os.environ.get('DYNAMODB_VERIFY', 'true') == 'true'
        return FullTextStoreSession(endpoint_url, aws_access_key,
                                    aws_secret_key, aws_session_token,
                                    region_name, verify=verify,
                                    version=version)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'fulltext_store'):
                ctx.fulltext_store = self.get_session()
            return ctx.fulltext_store
        return self.get_session()     # No application context.


store = FullTextStore()
