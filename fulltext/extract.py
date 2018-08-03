"""Provides asynchronous task for fulltext extraction."""

from celery import shared_task
import os
from datetime import datetime
from arxiv.base import logging
from fulltext.services import store, retrieve, fulltext
from fulltext.process import psv
from celery.result import AsyncResult
from celery import current_app
from celery.signals import after_task_publish
logger = logging.getLogger(__name__)


@shared_task
def extract_fulltext(document_id: str, pdf_url: str) -> None:
    """Perform fulltext extraction for a single arXiv document."""
    logger.info('Retrieving PDF for %s' % document_id)
    start_time = datetime.now()
    try:
        # Retrieve PDF from arXiv central document store.
        pdf_path = retrieve.retrieve(pdf_url, document_id)
        if pdf_path is None:
            raise RuntimeError('%s: no PDF available' % document_id)
        logger.info('%s: retrieved PDF' % document_id)

        logger.info('Attempting text extraction for %s' % document_id)
        content = fulltext.extract_fulltext(pdf_path)
        logger.info('Text extraction for %s succeeded with %i chars' %
                    (document_id, len(content)))
        try:
            store.store(document_id, content)
        except RuntimeError as e:   # TODO: flesh out exception states.
            raise
        duration = (start_time - datetime.now()).microseconds
        logger.info(f'Finished extraction for {document_id} in {duration} ms')

        psv_content = psv.normalize_text_psv(content)
        try:
            store.store(document_id, psv_content, content_format='psv')
        except RuntimeError as e:   # TODO: flesh out exception states.
            raise
        logger.info(f'Stored PSV normalized content for {document_id}')

        os.remove(pdf_path)    # Cleanup.

    except Exception as e:
        logger.error('Failed to process %s: %s' % (document_id, e))
        raise e
    return {
        'document_id': document_id,
    }


extract_fulltext.async_result = AsyncResult


@after_task_publish.connect
def update_sent_state(sender=None, headers=None, body=None, **kwargs):
    """Set state to SENT, so that we can tell whether a task exists."""
    task = current_app.tasks.get(sender)
    backend = task.backend if task else current_app.backend
    backend.store_result(headers['id'], None, "SENT")
