"""Provides a record processor for PDFIsAvailable notifications."""

from typing import Dict, List, Any, Optional, Tuple, Union
import json
import os
import time
import boto3
from botocore.exceptions import WaiterError, NoCredentialsError, \
    PartialCredentialsError, BotoCoreError, ClientError

from flask import url_for

from arxiv.base import logging
from arxiv.integration.kinesis import consumer
from arxiv.integration.kinesis.consumer import BaseConsumer, StopProcessing, \
    RestartProcessing
from arxiv.vault.manager import ConfigManager

from retry.api import retry_call

from fulltext import extract

logger = logging.getLogger(__name__)
# logger.propagate = True


class BadMessage(RuntimeError):
    """A malformed notification was encountered."""


class FulltextRecordProcessor(BaseConsumer):
    """Consumes ``PDFIsAvailable`` notifications, creates extraction tasks."""

    sleep = 0.2
    sleep_after_credentials = 10

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize a secrets manager before starting."""
        self._config = kwargs.pop('config', {})
        super(FulltextRecordProcessor, self).__init__(*args, **kwargs)
        if self._config.get('VAULT_ENABLED'):
            logger.info('Vault enabled; getting secrets')
            self._secrets = ConfigManager(self._config)
            self.update_secrets()
        self._access_key = self._config.get('AWS_ACCESS_KEY_ID')
        self._secret_key = self._config.get('AWS_SECRET_ACCESS_KEY')

    def wait_for_stream(self, tries: int = 5, delay: int = 5,
                        max_delay: Optional[int] = None, backoff: int = 2,
                        jitter: Union[int, Tuple[int, int]] = 0) -> None:
        """
        Wait for the stream to become available.

        If the stream becomes available, returns ``None``. Otherwise, raises
        a :class:`.StreamNotAvailable` exception.

        Raises
        ------
        :class:`.StreamNotAvailable`
            Raised when the stream could not be reached.

        """
        waiter = self.client.get_waiter('stream_exists')
        try:
            logger.info(f'Waiting for stream {self.stream_name}')
            waiter.wait(
                StreamName=self.stream_name,
                WaiterConfig=dict(
                    Delay=delay,
                    MaxAttempts=tries,
                    ExclusiveStartShardId=self.shard_id
                )
            )
        except WaiterError as e:
            msg = 'Failed to get stream while waiting'
            logger.error(msg)
            raise consumer.exceptions.StreamNotAvailable(msg) from e
        except (PartialCredentialsError, NoCredentialsError) as e:
            msg = 'Credentials missing or incomplete: %s'
            logger.error(msg, e.msg)
            raise consumer.exceptions.ConfigurationError(msg % e.msg) from e
        logger.info('Done waiting')

    def update_secrets(self) -> bool:
        """Update any secrets that are out of date."""
        got_new_secrets = False
        for key, value in self._secrets.yield_secrets():
            if self._config.get(key) != value:
                got_new_secrets = True
            self._config[key] = value
            os.environ[key] = str(value)
        self._access_key = self._config.get('AWS_ACCESS_KEY_ID')
        self._secret_key = self._config.get('AWS_SECRET_ACCESS_KEY')
        if got_new_secrets:
            logger.debug('Got new secrets')
        return got_new_secrets

    def process_records(self, start: str) -> Tuple[str, int]:
        """Update secrets before getting a new batch of records."""
        if self._config.get('VAULT_ENABLED') and self.update_secrets():
            # From the docs:
            #
            # > Unfortunately, IAM credentials are eventually consistent with
            # > respect to other Amazon services. If you are planning on using
            # > these credential in a pipeline, you may need to add a delay of
            # > 5-10 seconds (or more) after fetching credentials before they
            # > can be used successfully.
            #  -- https://www.vaultproject.io/docs/secrets/aws/index.html#usage
            time.sleep(self.sleep_after_credentials)
            raise RestartProcessing('Got fresh credentials')
        sup: Tuple[str, int] = \
            super(FulltextRecordProcessor, self).process_records(start)
        return sup

    def process_record(self, record: dict) -> None:
        """
        Call for each record that is passed to process_records.

        Parameters
        ----------
        data : bytes
        partition_key : bytes
        sequence_number : int
        sub_sequence_number : int

        Raises
        ------
        IndexingFailed
            Indexing of the document failed in a way that indicates recovery
            is unlikely for subsequent papers, or too many individual
            documents failed.

        """
        time.sleep(self.sleep)
        logger.debug(f'Processing record %s', record["SequenceNumber"])
        try:
            deserialized = json.loads(record['Data'].decode('utf-8'))
        except json.decoder.JSONDecodeError as e:
            logger.error("Error while deserializing data %s", e)
            logger.error("Data payload: %s", record['Data'])
            raise BadMessage('Could not deserialize payload')

        arxiv_id: str = deserialized.get('document_id')
        logger.info(f'Processing notification for %s', arxiv_id)
        extract.create_task(arxiv_id, 'arxiv')
