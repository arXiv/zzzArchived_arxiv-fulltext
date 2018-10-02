from typing import Optional, Tuple, Dict, Any
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest, \
    NotAcceptable, BadRequest
from arxiv.base import logging
from arxiv import status
from fulltext.services import store, pdf
from fulltext.extract import create_extraction_task, \
    get_extraction_task_result, get_extraction_task_status
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
        content_data = store.retrieve(paper_id, version=version,
                                      content_format=content_format,
                                      bucket=id_type)
    except IOError as e:
        raise InternalServerError('Could not connect to backend') from e
    except store.DoesNotExist as e:
        raise NotFound('No such extraction')
    except Exception as e:
        raise InternalServerError(f'Unhandled exception: {e}') from e

    # Extraction has already been requested.
    if 'placeholder' in content_data:
        if 'task_id' in content_data['placeholder']:
            task_id = content_data['placeholder']['task_id']
            location = url_for('fulltext.task_status', task_id=task_id)
            return ACCEPTED, status.HTTP_303_SEE_OTHER, {'Location': location}
        # It is possible that the extraction failed, in which case we simply
        # want to return whatever was stored at the end of the attempt.
        return content_data['placeholder'], status.HTTP_200_OK, {}
    return content_data, status.HTTP_200_OK, {}


def extract(paper_id: str, id_type: str = 'arxiv') -> Response:
    """Handle a request to force text extraction."""
    logger.info('extract: got paper_id: %s' % paper_id)
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

    task_id = create_extraction_task(paper_id, pdf_url, id_type)
    location = url_for('fulltext.task_status', task_id=task_id)
    return ACCEPTED, status.HTTP_202_ACCEPTED, {'Location': location}


def get_task_status(task_id: str) -> Response:
    """Check the status of an extraction request."""
    logger.debug('%s: Get status for task' % task_id)
    if not isinstance(task_id, str):
        logger.debug('%s: Failed, invalid task id' % task_id)
        raise BadRequest('task_id must be string, not %s' % type(task_id))

    task_status = get_extraction_task_status(task_id)
    logger.debug('%s: got result: %s' % (task_id, task_status))
    if task_status == 'PENDING':
        raise NotFound('task not found')
    elif task_status in ['SENT', 'STARTED', 'RETRY']:
        return TASK_IN_PROGRESS, status.HTTP_200_OK, {}
    elif task_status == 'FAILURE':
        task_result = get_extraction_task_result(task_id)
        logger.error('%s: failed task: %s' % (task_id, task_result))
        reason = TASK_FAILED
        reason.update({'reason': str(task_result)})
        return reason, status.HTTP_200_OK, {}
    elif task_status == 'SUCCESS':
        task_result = get_extraction_task_result(task_id)
        paper_id = task_result.get('paper_id')
        id_type = task_result.get('id_type')
        logger.debug('Retrieved result successfully, paper_id: %s' %
                     paper_id)
        if id_type == 'arxiv':
            target = url_for('fulltext.retrieve', paper_id=paper_id)
        elif id_type == 'submission':
            target = url_for('fulltext.retrieve_submission', paper_id=paper_id)
        else:
            raise NotFound('No such identifier')
        headers = {'Location': target}
        return TASK_COMPLETE, status.HTTP_303_SEE_OTHER, headers
    raise NotFound('task not found')
