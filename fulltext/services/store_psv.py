"""The request table tracks work on arXiv documents."""

from fulltext import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime
import os
from fulltext.context import get_application_config, get_application_global
from fulltext.services import credentials
import gzip
logger = logging.getLogger(__name__)


class PSVStoreSession(object):
    table_name = 'PSV'

    def __init__(self, endpoint_url: str, access_key: str, secret_key: str,
                 token: str, region_name: str, verify: bool=True,
                 version: float=0.0) -> None:
        logger.debug('New session with dynamodb at %s' % endpoint_url)
        self.version = version
        self.dynamodb = boto3.resource('dynamodb', verify=verify,
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=access_key,
                                       aws_secret_access_key=secret_key,
                                       aws_session_token=token)
        logger.debug('New dynamodb resource: %s' % str(id(self.dynamodb)))

        self.table = self.dynamodb.Table(self.table_name)
        logger.debug('New table object: %s' % str(id(self.table)))

    def create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        try:
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
                    'ReadCapacityUnits': 20,
                    'WriteCapacityUnits': 20
                }
            )
            waiter = table.meta.client.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)
            logger.debug('Created table: %s' % self.table_name)
        except ClientError as e:
            logger.debug('Table already exists: %s' % self.table_name)

    def create(self, document_id: str, content: str) -> None:
        """
        Store PSV format content for a document.

        Parameters
        ----------
        sequence_id : int
        state : str
        document_id : str

        Raises
        ------
        IOError
        """
        logger.debug('Store PSV for %s' % document_id)
        entry = {
            'document': document_id,
            'version': self.version,
            'created':  datetime.now().isoformat(),
            'content': gzip.compress(bytes(content, encoding='utf-8'))
        }
        try:
            self.table.put_item(Item=entry)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                self.create_table()
                try:
                    self.table.put_item(Item=entry)
                    return
                except ClientError as e:
                    raise IOError('Failed to create: %s' % e) from e
            else:
                raise IOError('Failed to create: %s' % e) from e
        logger.debug('Store record for %s successful' % document_id)

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
        query = {
            'Limit': 1,
            'ScanIndexForward': False,
            'KeyConditionExpression': Key('document').eq(document_id)
        }
        try:
            response = self.table.query(**query)
            logger.debug('Got response with %i results' %
                         len(response['Items']))
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                self.create_table()
                try:
                    response = self.table.query(**query)
                except ClientError as e:
                    raise IOError('Could not connect to store: %s' % e)
            else:
                raise IOError('Could not connect to store: %s' % e)
        if len(response['Items']) == 0:
            return None
        item = response['Items'][0]
        return {
            'document': item['document'],
            'version': float(item.get('version')),
            'created': item['created'],
            'content': gzip.decompress(item['content'].value).decode('utf-8')
        }


def init_app(app) -> None:
    app.config.setdefault('DYNAMODB_ENDPOINT', None)
    app.config.setdefault('AWS_REGION', 'us-east-1')
    app.config.setdefault('VERSION', "0.0")
    app.config.setdefault('DYNAMODB_VERIFY', 'true')


def get_session(app: object=None) -> PSVStoreSession:
    config = get_application_config(app)
    creds = credentials.current_session()
    try:
        access_key, secret_key, token = creds.get_credentials()
    except IOError as e:
        access_key, secret_key, token = None, None, None
        logger.debug('failed to load instance credentials: %s', str(e))
    if access_key is None or secret_key is None:
        access_key = config.get('AWS_ACCESS_KEY_ID', None)
        secret_key = config.get('AWS_SECRET_ACCESS_KEY', None)
        token = config.get('AWS_SESSION_TOKEN', None)

    endpoint_url = config.get('DYNAMODB_ENDPOINT', None)
    region_name = config.get('AWS_REGION', 'us-east-1')
    version = config.get('VERSION', "0.0")
    verify = bool(config.get('DYNAMODB_VERIFY', "true"))
    return PSVStoreSession(endpoint_url, access_key, secret_key, token,
                           region_name, verify=verify, version=version)


def current_session():
    """Get/create :class:`.PSVStoreSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'psv_store' not in g:
        g.psv_store = get_session()
    return g.psv_store


def create(document_id: str, content: str) -> None:
    """Store PSV format content for a document."""
    return current_session().create(document_id, content)


def latest(document_id: str) -> dict:
    """Retrieve the most recent extraction for a document."""
    return current_session().latest(document_id)


def current_version() -> str:
    """Get the current extraction version."""
    return current_session().version


def init_db():
    """Create datastore tables."""
    session = current_session()
    session.create_table()
