from celery import shared_task
import os
import tempfile
import requests
from datetime import datetime
from fulltext import logging
from fulltext.services.fulltext import extractor
from fulltext.services.store import store
from fulltext.services.events import events
from fulltext.services.metrics import metrics

logger = logging.getLogger(__name__)


def retrieve(document_id: str) -> tuple:
    """
    Retrieve PDF for an arXiv document.

    Parameters
    ----------
    document_id : str

    Returns
    -------
    pdf_path : str
    source_path : str
    """
    pdf_response = requests.get('https://arxiv.org/pdf/%s.pdf' % document_id)
    if pdf_response.status_code == requests.codes.NOT_FOUND:
        logger.info('Could not retrieve PDF for %s' % document_id)
        return
    elif pdf_response.status_code != requests.codes.ok:
        raise IOError('Unexpected status for %s PDF' % document_id)

    _, pdf_path = tempfile.mkstemp(prefix=document_id, suffix='.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(pdf_response.content)
    os.chmod(pdf_path, 0o775)
    return pdf_path


@shared_task
def extract_fulltext(document_id: str, sequence_number: int) -> None:
    logger.info('Retrieving PDF for %s' % document_id)
    start_time = datetime.now()
    try:
        pdf_path = retrieve(document_id)
        if pdf_path is None:
            logger.info('Could not retrieve PDF for %s' % document_id)
            metrics.session.report('PDFIsAvailable', 0.)
            create_failed_event(document_id, sequence_number)
            return
    except Exception as e:
        logger.error(str(e))
        metrics.session.report('PDFIsAvailable', 0.)
        create_failed_event(document_id, sequence_number)
        return
    metrics.session.report('PDFIsAvailable', 1.)

    logger.info('Attempting text extraction for %s' % document_id)
    try:
        txt_path = extractor.session.extract_fulltext(pdf_path)
    except Exception as e:
        logger.error('Extraction for %s failed with: %s' % (document_id, e))
        create_failed_event(document_id, sequence_number)
        return
    logger.info('Text extraction for %s succeeded with %s' % (document_id,
                                                              txt_path))
    os.remove(pdf_path)    # Cleanup.
    with open(txt_path, encoding='utf-8') as f:
        content = f.read()
        try:
            store.session.create(document_id, content)
        except Exception as e:
            msg = 'Failed to store fulltext for %s: %s' % (document_id, e)
            logger.error(msg)
            create_failed_event(document_id, sequence_number)

    os.remove(txt_path)    # Cleanup.
    end_time = datetime.now()
    metrics.session.report('ProcessingDuration',
                           (start_time - end_time).microseconds,
                           units='Microseconds')
    create_success_event(document_id, sequence_number)
    return


def create_failed_event(document_id: str, sequence_id: int, *args) -> dict:
    """Commemorate extraction failure."""
    metrics.session.report('ProcessingSucceeded', 0.)
    try:
        event_data = events.session.create(sequence_id,
                                           state=events.session.FAILED,
                                           document_id=document_id)
    except IOError as e:
        msg = 'Failed to store failed state for %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg)
    return event_data


def create_success_event(document_id: str, sequence_id: int=-1) -> dict:
    """Commemorate extraction success."""
    metrics.session.report('ProcessingSucceeded', 1.)
    if sequence_id == -1:   # Legacy message.
        return
    try:
        data = events.session.create(sequence_id,
                                     state=events.session.COMPLETED,
                                     document_id=document_id)
    except IOError as e:
        msg = 'Failed to store success state for %s: %s' % (document_id, e)
        logger.error(msg)
        raise RuntimeError(msg)
    return data
