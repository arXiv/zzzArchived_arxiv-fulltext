"""API controllers."""

from typing import Optional, Tuple, Dict, Any, Callable
from http import HTTPStatus as status

from flask import url_for
from celery import current_app
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest, \
    NotAcceptable

from arxiv.base import logging

from .services import store, pdf, compiler
from .extract import create_extraction_task, get_extraction_task, \
    extraction_task_exists, get_version, NoSuchTask, TaskCreationFailed
from .domain import Extraction

logger = logging.getLogger(__name__)

ACCEPTED = {'reason': 'fulltext extraction in process'}
ALREADY_EXISTS = {'reason': 'extraction already exists'}
TASK_IN_PROGRESS = {'status': 'in progress'}
TASK_FAILED = {'status': 'failed'}
TASK_COMPLETE = {'status': 'complete'}

ARXIV = 'arxiv'
SUBMISSION = 'submission'

Response = Tuple[Dict[str, Any], int, Dict[str, Any]]
Authorizer = Callable[[str, Optional[str]], bool]


def service_status() -> Response:
    """Handle a request for the status of this service."""
    if store.Storage.ready():   # This is the critical upstream integration.
        return {}, status.OK, {}
    raise InternalServerError('Failed readiness check')


def retrieve(identifier: str, id_type: str = 'arxiv',
             version: Optional[str] = None,
             content_format: str = 'plain',
             authorizer: Optional[Authorizer] = None) -> Response:
    """
    Handle request for full-text content for an arXiv e-print.

    Parameters
    ----------
    identifier : str
        Identifier for an arXiv resource, usually a published e-print.
    id_type : str
        The type of identifier that is `identifier`.
    version : str or None
        If provided, the desired extraction version.
    content_format : str
        The desired content format (default: `plain`).

    Returns
    -------
    tuple

    """
    if id_type not in [ARXIV, SUBMISSION]:
        raise NotFound('Unrecognized identifier')

    try:
        product = store.Storage.retrieve(identifier, version, content_format,
                                         id_type)
    except IOError as e:
        raise InternalServerError('Could not connect to backend') from e
    except store.DoesNotExist:
        # If there is only metadata, we should still get a response from
        # the store. So if we hit DoesNotExist there really is nothing to see
        # here folks, move along.
        raise NotFound('No such extraction')

    # Make sure that the client is authorized to work with this resource before
    # doing anything else.
    if authorizer and not authorizer(identifier, product.owner):
        raise NotFound('No such extraction')

    if product.content is None \
            and product.status is Extraction.Status.IN_PROGRESS:
        target = url_for('fulltext.task_status', identifier=identifier,
                         id_type=id_type)
        return TASK_IN_PROGRESS, status.SEE_OTHER, {'Location': target}
    return product.to_dict(), status.OK, {}


def extract(id_type: str, identifier: str, token: str, force: bool = False,
            authorizer: Optional[Authorizer] = None) -> Response:
    """Handle a request to force text extraction."""
    if id_type not in [ARXIV, SUBMISSION]:
        raise NotFound('Unsupported identifier')

    if not force:
        # If an extraction product or task already exists for this paper,
        # redirect. Don't do the same work twice for a given version of the
        # extractor.
        product: Optional[Extraction] = None
        try:
            product = store.Storage.retrieve(identifier, bucket=id_type,
                                             meta_only=True)
        except IOError as e:
            raise InternalServerError('Could not connect to backend') from e
        except store.DoesNotExist:
            pass

        if product is not None:
            # Redirect to either the task status endpoint or the finished
            # extraction product.
            return _redirect(product, authorizer)

    logger.debug('No existing task nor extraction for %s', identifier)
    # Before creating an extraction task, check that the intended document
    # even exists. This gives the client a clear failure now, rather than
    # waiting until the async task fails. At the same time, we'll also grab
    # the owner (if there is one) so that we can authorize the request.
    owner: Optional[str] = None
    if id_type == ARXIV and not pdf.CanonicalPDF.exists(identifier):
        raise NotFound('No such document')
    elif id_type == SUBMISSION:
        try:
            owner = compiler.Compiler.owner(identifier, token)
            logger.debug('Got owner %s', owner)
        except compiler.NotFound as e:
            logger.debug('Compiler returned 404 Not Found for %s', identifier)
            raise NotFound('No such document') from e

    # Make sure that the client is authorized to work with this resource.
    # The route may have passed in an authorizer function that works with
    # the auth API to authorize the request.
    if authorizer is not None and not authorizer(identifier, owner):
        logger.debug('Client is not authorized to work with this resource')
        # Pretend that the resource does not even exist.
        raise NotFound('No such document')

    try:
        logger.debug('Create a new extraction task with %s, %s',
                     identifier, id_type)
        create_extraction_task(identifier, id_type, owner, token)
    except TaskCreationFailed as e:
        raise InternalServerError('Could not start extraction') from e
    target = url_for('fulltext.task_status', identifier=identifier,
                     id_type=id_type)
    return ACCEPTED, status.ACCEPTED, {'Location': target}


def get_task_status(identifier: str, id_type: str = 'arxiv',
                    version: Optional[str] = None,
                    authorizer: Optional[Authorizer] = None) -> Response:
    """Check the status of an extraction request."""
    logger.debug('%s: Get status for paper' % identifier)
    if id_type not in [ARXIV, SUBMISSION]:
        raise NotFound('Unsupported identifier')

    try:
        product = store.Storage.retrieve(identifier, version, id_type,
                                         meta_only=True)
    except IOError as e:
        raise InternalServerError('Could not connect to backend') from e
    except store.DoesNotExist:
        # If there is only metadata, we should still get a response from
        # the store. So if we hit DoesNotExist there really is nothing to see
        # here folks, move along.
        raise NotFound('No such task')

    # Make sure that the client is authorized to work with this resource before
    # doing anything else.
    if authorizer and not authorizer(identifier, product.owner):
        raise NotFound('No such task')

    if product.status is Extraction.Status.SUCCEEDED:
        logger.debug('Task for %s is already complete', identifier)
        target = url_for('fulltext.retrieve', identifier=identifier,
                         id_type=id_type)
        return TASK_COMPLETE, status.SEE_OTHER, {'Location': target}

    try:
        task = get_extraction_task(identifier, id_type, version)
    except NoSuchTask as e:
        raise NotFound('No such task') from e
    return _task_redirect(task)


def _redirect(extraction: Extraction, authorizer: Authorizer) -> Response:
    # Make sure that the client is authorized to work with this
    # resource before redirecting.
    if authorizer and not authorizer(extraction.identifier, extraction.owner):
        logger.debug('Requester is not authorized')
        raise NotFound('No such extraction')

    if extraction.status is Extraction.Status.IN_PROGRESS:
        logger.debug('Extraction in progress')
        target = url_for('fulltext.task_status',
                         identifier=extraction.identifier,
                         id_type=extraction.bucket)
        data = {'reason': 'extraction in progress'}
    else:
        logger.debug('Extraction already completed')
        target = url_for('fulltext.retrieve',
                         identifier=extraction.identifier,
                         id_type=extraction.bucket)
        data = {'reason': 'extraction already complete'}
    return data, status.SEE_OTHER, {'Location': target}


def _task_redirect(task: Extraction) -> Response:
    if task.status is Extraction.Status.IN_PROGRESS:
        return TASK_IN_PROGRESS, status.OK, {}
    elif task.status is Extraction.Status.FAILED:
        logger.error('%s: failed task: %s' % (task.task_id, task.result))
        reason = TASK_FAILED
        reason.update({'reason': str(task.result)})
        return reason, status.OK, {}
    elif task.status is Extraction.Status.SUCCEEDED:
        logger.debug('Task for %s is already complete', task.identifier)
        target = url_for('fulltext.retrieve', identifier=task.identifier,
                         id_type=task.bucket)
        return TASK_COMPLETE, status.SEE_OTHER, {'Location': target}
    raise InternalServerError('Unexpected state')
