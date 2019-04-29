"""Provides a record processor for PDFIsAvailable notifications."""

from typing import Dict, List, Any, Optional, Tuple
import json
import os
import time

from flask import url_for

from arxiv.base import logging
from arxiv.integration.kinesis.consumer import BaseConsumer, RestartProcessing
from arxiv.vault.manager import ConfigManager

from fulltext.extract import extract_fulltext

logger = logging.getLogger(__name__)
# logger.propagate = True


class BadMessage(RuntimeError):
    """A malformed notification was encountered."""


class FulltextRecordProcessor(BaseConsumer):
    """Consumes ``PDFIsAvailable`` notifications, creates extraction tasks."""

    sleep = 0.2

    def __init__(self, *args, **kwargs) -> None:
        """Initialize a secrets manager before starting."""
        self._config = kwargs.pop('config', {})
        super(FulltextRecordProcessor, self).__init__(*args, **kwargs)
        if self._config.get('VAULT_ENABLED'):
            logger.info('Vault enabled; getting secrets')
            self.__secrets = ConfigManager(self._config)
            self.update_secrets()
        self._access_key = self._config.get('AWS_ACCESS_KEY_ID')
        self._secret_key = self._config.get('AWS_SECRET_ACCESS_KEY')

    def update_secrets(self) -> bool:
        """Update any secrets that are out of date."""
        got_new_secrets = False
        for key, value in self.__secrets.yield_secrets():
            if self._config.get(key) != value:
                got_new_secrets = True
            self._config[key] = value
            os.environ[key] = str(value)
        self._access_key = self._config.get('AWS_ACCESS_KEY_ID')
        self._secret_key = self._config.get('AWS_SECRET_ACCESS_KEY')
        if got_new_secrets:
            time.sleep(0.5)
        return got_new_secrets

    def process_records(self, start: str) -> Tuple[str, int]:
        """Update secrets before getting a new batch of records."""
        if self._config.get('VAULT_ENABLED') and self.update_secrets():
            raise RestartProcessing('Got fresh credentials')
        return super(FulltextRecordProcessor, self).process_records(start)

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
        logger.debug(f'Processing record {record["SequenceNumber"]}')
        try:
            deserialized = json.loads(record['Data'].decode('utf-8'))
        except json.decoder.JSONDecodeError as e:
            logger.error("Error while deserializing data %s", e)
            logger.error("Data payload: %s", record['Data'])
            raise BadMessage('Could not deserialize payload')

        arxiv_id: str = deserialized.get('document_id')
        logger.info(f'Processing notification for {arxiv_id}')
        extract_fulltext.delay(arxiv_id, url_for('pdf', paper_id=arxiv_id))
