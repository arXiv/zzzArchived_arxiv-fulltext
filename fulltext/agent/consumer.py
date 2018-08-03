"""Provides a record processor for PDFIsAvailable notifications."""

from typing import Dict, List, Any, Optional
import json
import os
import time

from flask import url_for

from arxiv.base import logging
from arxiv.base.agent import BaseConsumer

from fulltext.extract import extract_fulltext

logger = logging.getLogger(__name__)
logger.propagate = False


class BadMessage(RuntimeError):
    """A malformed notification was encountered."""


class FulltextRecordProcessor(BaseConsumer):
    """Consumes ``PDFIsAvailable`` notifications, creates extraction tasks."""

    sleep = 0.1

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
