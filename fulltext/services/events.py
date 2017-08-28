"""The request table tracks work on arXiv documents."""

from fulltext import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime
import os
from flask import _app_ctx_stack as stack

logger = logging.getLogger(__name__)


class ExtractionEventSession(object):
    """Commemorates events for exaction on arXiv documents."""

    table_name = 'FullTextExtractionEvents'

    REQUESTED = 'REQU'
    FAILED = 'FAIL'
    COMPLETED = 'COMP'
    STATES = (REQUESTED, FAILED, COMPLETED)

    def __init__(self, endpoint_url: str, aws_access_key: str,
                 aws_secret_key: str, region_name: str) -> None:
        """Set up remote table."""
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name=region_name,
                                       endpoint_url=endpoint_url,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key)
        try:
            self._create_table()
        except ClientError as e:
            # logger.info('Table already exists: %s' % self.table_name)
            pass
        self.table = self.dynamodb.Table(self.table_name)

    def _create_table(self) -> None:
        """Set up a new table in DynamoDB. Blocks until table is available."""
        table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {'AttributeName': 'sequence', 'KeyType': 'HASH'},
                {'AttributeName': 'created', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {"AttributeName": 'sequence', "AttributeType": "S"},
                {"AttributeName": 'created', "AttributeType": "S"}
            ],
            ProvisionedThroughput={    # TODO: make this configurable.
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        waiter = table.meta.client.get_waiter('table_exists')
        waiter.wait(TableName=self.table_name)

    def create(self, sequence_id: int, state: str=REQUESTED,
               document_id: str=None, **extra) -> None:
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
        if state not in ExtractionEventSession.STATES:
            raise ValueError('Invalid state: %s' % state)

        entry = dict(extra)
        entry.update({
            'sequence': str(sequence_id),
            'created': datetime.now().isoformat(),
            'document': document_id,
            'state': state,
        })
        try:
            self.table.put_item(Item=entry)
        except ClientError as e:
            raise IOError('Failed to create: %s' % e) from e

    def latest(self, sequence_id: int) -> dict:
        """
        Retrieve the most recent event for a notification.

        Parameters
        ----------
        sequence_id : int

        Returns
        -------
        dict
        """
        response = self.table.query(
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression=Key('sequence').eq(sequence_id)
        )
        if len(response['Items']) == 0:
            return None
        return response['Items'][0]


class ExtractionEvents(object):
    """Extraction event store service integration."""

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.config.setdefault('FULLTEXT_DYNAMODB_ENDPOINT', None)
        app.config.setdefault('AWS_ACCESS_KEY_ID', 'asdf1234')
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', 'fdsa5678')
        app.config.setdefault('FULLTEXT_AWS_REGION', 'us-east-1')

    def get_session(self) -> None:
        try:
            endpoint_url = self.app.config['FULLTEXT_DYNAMODB_ENDPOINT']
            aws_access_key = self.app.config['AWS_ACCESS_KEY_ID']
            aws_secret_key = self.app.config['AWS_SECRET_ACCESS_KEY']
            region_name = self.app.config['FULLTEXT_AWS_REGION']
        except (RuntimeError, AttributeError) as e:    # No app context.
            endpoint_url = os.environ.get('FULLTEXT_DYNAMODB_ENDPOINT', None)
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'asdf')
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'fdsa')
            region_name = os.environ.get('FULLTEXT_AWS_REGION', 'us-east-1')
        return ExtractionEventSession(endpoint_url, aws_access_key,
                                      aws_secret_key, region_name)

    @property
    def session(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'extraction_events'):
                ctx.extraction_events = self.get_session()
            return ctx.extraction_events
        return self.get_session()     # No application context.


events = ExtractionEvents()
