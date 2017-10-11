"""Provides asynchronous task for fulltext extraction."""

from celery import shared_task
import os
from datetime import datetime
from fulltext import logging
from fulltext.services.fulltext import extractor
from fulltext.services.store import store
from fulltext.services.metrics import metrics
from fulltext.services.retrieve import retrievePDF
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
        pdf_path = retrievePDF.session.retrieve(pdf_url, document_id)
        if pdf_path is None:
            metrics.session.report('PDFIsAvailable', 0.)
            msg = '%s: no PDF available' % document_id
            logger.info(msg)
            raise RuntimeError(msg)
        metrics.session.report('PDFIsAvailable', 1.)
        logger.info('%s: retrieved PDF' % document_id)

        logger.info('Attempting text extraction for %s' % document_id)
        txt_path = extractor.session.extract_fulltext(pdf_path)
        logger.info('Text extraction for %s succeeded with %s' %
                    (document_id, txt_path))

        os.remove(pdf_path)    # Cleanup.
        with open(txt_path, encoding='utf-8') as f:
            content = f.read()
            store.session.create(document_id, content)

        os.remove(txt_path)    # Cleanup.
        end_time = datetime.now()
        metrics.session.report('ProcessingDuration',
                               (start_time - end_time).microseconds,
                               units='Microseconds')

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
