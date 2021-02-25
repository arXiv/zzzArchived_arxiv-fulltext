"""API controllers."""

from typing import Optional, Tuple, Dict, Any, Callable
from http import HTTPStatus as status

from flask import url_for
from celery import current_app
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest, \
    NotAcceptable

from arxiv.base import logging

from .services import store, legacy, preview
from . import extract
from .domain import Extraction, Status\
    , SupportedFormats, SupportedBuckets

logger = logging.getLogger(__name__)

ACCEPTED = {'reason': 'fulltext extraction in process'}
ALREADY_EXISTS = {'reason': 'extraction already exists'}
TASK_IN_PROGRESS = {'status': Status.IN_PROGRESS.value}
TASK_FAILED = {'status': Status.FAILED.value}
TASK_COMPLETE = {'status': Status.SUCCEEDED.value}

Response = Tuple[Dict[str, Any], int, Dict[str, Any]]
Authorizer = Callable[[str, Optional[str]], bool]


def service_status() -> Response:
    """Handle a request for the status of this service."""
    # This is the critical upstream integration.
    stat = {
        'storage': store.Storage.current_session().is_available(),
        'extractor': extract.is_available(await_result=True)
    }
    if all(stat.values()):
        return stat, status.OK, {}
    raise InternalServerError(stat)    # type: ignore


def retrieve(identifier: str,                                # arch: controller
             id_type: str = SupportedBuckets.ARXIV,
             version: Optional[str] = None,
             content_fmt: str = SupportedFormats.PLAIN,
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
    content_fmt : str
        The desired content format (default: `plain`).

    Returns
    -------
    tuple

    """
    if id_type not in SupportedBuckets:
        raise NotFound('Unrecognized identifier')
    if content_fmt not in SupportedFormats:
        raise NotFound('Unsupported format')

    storage = store.Storage.current_session()
    try:
        product = storage.retrieve(identifier, version, content_fmt, id_type)
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
            and product.status is Status.IN_PROGRESS:
        target = url_for('fulltext.task_status', identifier=identifier,
                         id_type=id_type)
        return TASK_IN_PROGRESS, status.SEE_OTHER, {'Location': target}
    return product.to_dict(), status.OK, {}


def start_extraction(id_type: str, identifier: str, token: str,
                     force: bool = False,
                     authorizer: Optional[Authorizer] = None) -> Response:
    """Handle a request to force text extraction."""
    if id_type not in SupportedBuckets:
        raise NotFound('Unsupported identifier')

    canonical = legacy.CanonicalPDF.current_session()
    storage = store.Storage.current_session()
    previews = preview.PreviewService.current_session()

    # Before creating an extraction task, check that the intended document
    # even exists. This gives the client a clear failure now, rather than
    # waiting until the async task fails. At the same time, we'll also grab
    # the owner (if there is one) so that we can authorize the request.
    owner: Optional[str] = None
    if id_type == SupportedBuckets.ARXIV:
        if not canonical.exists(identifier):
            logger.debug('No PDF for this resource exists')
            raise NotFound('No such document')

        # Make sure that the client is authorized to work with this resource.
        # The route may have passed in an authorizer function that works with
        # the auth API to authorize the request.
        if authorizer is not None and not authorizer(identifier, owner):
            logger.debug('Client is not authorized to work with this resource')
            # Pretend that the resource does not even exist.
            raise NotFound('No such document')
    elif id_type == SupportedBuckets.SUBMISSION:
        try:
            owner = previews.get_owner(identifier, token)
            logger.debug('Got owner %s', owner)
        except preview.exceptions.NotFound as e:
            logger.debug('Preview returned 404 Not Found for %s', identifier)
            raise NotFound('No such document') from e

        # Make sure that the client is authorized to work with this resource.
        # For submissions, an authorizer function must be provided, so we
        # should deny access if one is mistakenly not provided.

        # TODO: auth disabled
        # if authorizer is None or not authorizer(identifier, owner):
        #     logger.debug('Client is not authorized to work with this resource')
        #     # Pretend that the resource does not even exist.
        #     raise NotFound('No such document')
    else:
        raise NotFound('No such document')

    if not force:
        # If an extraction product or task already exists for this paper,
        # redirect. Don't do the same work twice for a given version of the
        # extractor.
        logger.debug('Check for an existing product or task')
        product: Optional[Extraction] = None
        try:
            product = storage.retrieve(identifier, bucket=id_type,
                                       meta_only=True)
        except IOError as e:
            raise InternalServerError('Could not connect to backend') from e
        except store.DoesNotExist:
            pass

        if product is not None:
            # Redirect to either the task status endpoint or the finished
            # extraction product.
            logger.debug('Got an extraction product: %s', product)
            return _redirect(product, authorizer)

    logger.debug('No existing task nor extraction for %s', identifier)

    try:
        logger.debug('Create a new extraction task with %s, %s',
                     identifier, id_type)
        extract.create_task(identifier, id_type, owner, token)
    except extract.TaskCreationFailed as e:
        raise InternalServerError('Could not start extraction') from e
    target = url_for('fulltext.task_status', identifier=identifier,
                     id_type=id_type)
    return ACCEPTED, status.ACCEPTED, {'Location': target}


def get_task_status(identifier: str, id_type: str = SupportedBuckets.ARXIV,
                    version: Optional[str] = None,
                    authorizer: Optional[Authorizer] = None) -> Response:
    """Check the status of an extraction request."""
    logger.debug('get task status for %s in %s', identifier, id_type)
    if id_type not in SupportedBuckets:
        logger.debug('unsupported identifier type %s', id_type)
        raise NotFound('Unsupported identifier')

    storage = store.Storage.current_session()
    try:
        product = storage.retrieve(identifier, version, bucket=id_type,
                                   meta_only=True)
    except IOError as e:
        logger.error('could not connect to storage backend')
        raise InternalServerError('Could not connect to backend') from e
    except store.DoesNotExist:
        # If there is only metadata, we should still get a response from
        # the store. So if we hit DoesNotExist there really is nothing to see
        # here folks, move along.
        logger.debug('store says does not exist')
        raise NotFound('No such task')

    # Make sure that the client is authorized to work with this resource before
    # doing anything else.
    if authorizer and not authorizer(identifier, product.owner):
        logger.debug('requester not authorized; return 404 Not Found')
        raise NotFound('No such task')

    logger.debug('Task has status: %s', product.status)

    if product.status is Status.SUCCEEDED:
        logger.debug('Task for %s is already complete', identifier)
        target = url_for('fulltext.retrieve', identifier=identifier,
                         id_type=id_type)
        return product.to_dict(), status.SEE_OTHER, {'Location': target}

    if version is None:
        version = extract.get_version()
    try:
        task = extract.get_task(identifier, id_type, version)
    except extract.NoSuchTask as e:
        logger.debug(f'No such task: {e}')
        # raise NotFound('No such task') from e
        task = product
    return _task_redirect(task, product)


def _redirect(extraction: Extraction,
              authorizer: Optional[Authorizer]) -> Response:
    # Make sure that the client is authorized to work with this
    # resource before redirecting.
    if authorizer and not authorizer(extraction.identifier, extraction.owner):
        logger.debug('Requester is not authorized')
        raise NotFound('No such extraction')

    if Status is Status.IN_PROGRESS:
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


def _task_redirect(task: Extraction, product: Extraction) -> Response:
    data: Dict[str, Any] = product.to_dict()
    code: int = status.OK
    headers: Dict[str, str] = {}
    if task.status is Status.IN_PROGRESS:
        data.update(TASK_IN_PROGRESS)
    elif task.status is Status.FAILED:
        logger.error('%s: failed task: %s', task.task_id, task.exception)
        data.update({'reason': str(task.exception)})
    elif task.status is Status.SUCCEEDED:
        logger.debug('Task for %s is already complete', task.identifier)
        target = url_for('fulltext.retrieve', identifier=task.identifier,
                         id_type=task.bucket)
        data.update(TASK_COMPLETE)
        code = status.SEE_OTHER
        headers = {'Location': target}
    return data, code, headers
