from typing import Optional, Tuple, Dict, Any
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest, \
    NotAcceptable, BadRequest
from arxiv.base import logging
from arxiv import status

from .domain import ExtractionPlaceholder, ExtractionTask, ExtractionProduct
from fulltext.services import store, pdf
from fulltext.extract import create_extraction_task, get_extraction_task, \
    extraction_task_exists, get_version, NoSuchTask, TaskCreationFailed
from flask import url_for
from celery import current_app


logger = logging.getLogger(__name__)

ACCEPTED = {'reason': 'fulltext extraction in process'}
ALREADY_EXISTS = {'reason': 'extraction already exists'}
TASK_IN_PROGRESS = {'status': 'in progress'}
TASK_FAILED = {'status': 'failed'}
TASK_COMPLETE = {'status': 'complete'}
HTTP_202_ACCEPTED = 202
HTTP_303_SEE_OTHER = 303
HTTP_400_BAD_REQUEST = 400
HTTP_404_NOT_FOUND = 404
HTTP_500_INTERNAL_SERVER_ERROR = 500

Response = Tuple[Dict[str, Any], int, Dict[str, Any]]


def service_status() -> Response:
    """Handle a request for the status of this service."""
    if store.ready():   # This is the critical upstream integration.
        return {}, status.HTTP_200_OK, {}
    raise InternalServerError('Failed readiness check')


def retrieve(paper_id: str, id_type: str = 'arxiv',
             version: Optional[str] = None,
             content_format: str = 'plain') -> Response:
    """
    Handle request for full-text content for an arXiv paper.

    Parameters
    ----------
    paper_id : str
        Identifier for an arXiv resource, usually a published e-print.
    id_type : str
        The type of identifier that is `paper_id`.
    version : str or None
        If provided, the desired extraction version.
    content_format : str
        The desired content format (default: `plain`).

    Returns
    -------
    tuple
    """
    try:
        product = store.retrieve(paper_id, version=version,
                                 content_format=content_format,
                                 bucket=id_type)

    except IOError as e:
        raise InternalServerError('Could not connect to backend') from e
    except store.DoesNotExist as e:
        # Check whether there is a task in progress for this paper.
        if extraction_task_exists(paper_id, id_type, version):
            if id_type == 'arxiv':
                status_endpoint = 'fulltext.task_status'
            elif id_type == 'submission':
                status_endpoint = 'fulltext.submission_task_status'
            headers = {'Location': url_for(status_endpoint, paper_id=paper_id)}
            return TASK_IN_PROGRESS, status.HTTP_303_SEE_OTHER, headers
        raise NotFound('No such extraction')
    except Exception as e:
        raise InternalServerError(f'Unhandled exception: {e}') from e
    return product.to_dict(), status.HTTP_200_OK, {}


def extract(paper_id: str, id_type: str = 'arxiv') -> Response:
    """Handle a request to force text extraction."""
    logger.info('extract: got paper_id: %s' % paper_id)
    if id_type == 'arxiv':
        status_endpoint = 'fulltext.task_status'
    elif id_type == 'submission':
        status_endpoint = 'fulltext.submission_task_status'

    # If an extraction task already exists for this paper, redirect. Don't
    # create the same task twice.
    if extraction_task_exists(paper_id, id_type):
        status_code = status.HTTP_303_SEE_OTHER

    # Otherwise, we have a task to create.
    else:
        # Before creating an extraction task, check that the intended document
        # even exists. This gives the client a clear failure now, rather than
        # waiting until the async task fails.
        if id_type == 'arxiv':
            pdf_url = url_for('pdf', paper_id=paper_id)
        elif id_type == 'submission':
            pdf_url = url_for('submission_pdf', submission_id=paper_id)
        else:
            raise NotFound('Invalid identifier')
        if not pdf.exists(pdf_url):
            raise NotFound('No such document')

        status_code = status.HTTP_202_ACCEPTED
        try:
            create_extraction_task(paper_id, pdf_url, id_type)
        except TaskCreationFailed as e:
            raise InternalServerError('Could not start extraction') from e

    headers = {'Location': url_for(status_endpoint, paper_id=paper_id)}
    return ACCEPTED, status_code, headers


def get_task_status(paper_id: str, id_type: str = 'arxiv',
                    version: Optional[str] = None) -> Response:
    """Check the status of an extraction request."""
    logger.debug('%s: Get status for paper' % paper_id)

    try:
        task = get_extraction_task(paper_id, id_type, version)
    except NoSuchTask as e:
        raise NotFound('No such task') from e

    logger.debug('%s: got task: %s' % (task.task_id, task))
    if task.status is ExtractionTask.Statuses.IN_PROGRESS:
        return TASK_IN_PROGRESS, status.HTTP_200_OK, {}
    elif task.status is ExtractionTask.Statuses.FAILED:
        logger.error('%s: failed task: %s' % (task.task_id, task.result))
        reason = TASK_FAILED
        reason.update({'reason': task.result})
        return reason, status.HTTP_200_OK, {}
    elif task.status is ExtractionTask.Statuses.SUCCEEDED:
        logger.debug('Retrieved result successfully, paper_id: %s',
                     task.paper_id)
        if task.id_type == 'arxiv':
            target = url_for('fulltext.retrieve', paper_id=task.paper_id)
        elif task.id_type == 'submission':
            target = url_for('fulltext.retrieve_submission',
                             paper_id=task.paper_id)
        else:
            raise NotFound('No such identifier')
        headers = {'Location': target}
        return TASK_COMPLETE, status.HTTP_303_SEE_OTHER, headers
    raise NotFound('task not found')
