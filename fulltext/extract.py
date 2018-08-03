"""Provides asynchronous task for fulltext extraction."""

from celery import shared_task
import os
from datetime import datetime
from arxiv.base import logging
from fulltext.services import store, retrieve, fulltext, metrics
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
            metrics.report('PDFIsAvailable', 0.)
            msg = '%s: no PDF available' % document_id
            logger.info(msg)
            raise RuntimeError(msg)
        metrics.report('PDFIsAvailable', 1.)
        logger.info('%s: retrieved PDF' % document_id)

        logger.info('Attempting text extraction for %s' % document_id)
        content = fulltext.extract_fulltext(pdf_path)
        logger.info('Text extraction for %s succeeded with %i chars' %
                    (document_id, len(content)))

        os.remove(pdf_path)    # Cleanup.
        store.create(document_id, content)
        duration = (start_time - datetime.now()).microseconds
        metrics.report('ProcessingDuration', duration, units='Microseconds')
        logger.debug('Finished processing in %i microseconds', duration)

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
