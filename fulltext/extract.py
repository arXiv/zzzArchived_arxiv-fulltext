"""Provides asynchronous task for fulltext extraction."""

import os
import tempfile
from typing import Tuple, Optional, Dict, Any, IO
from datetime import datetime
from pytz import UTC
import shutil

from celery import Celery
from celery.result import AsyncResult
from celery.signals import after_task_publish
from flask import Flask, current_app

from arxiv.integration.api.exceptions import RequestFailed
from arxiv.base.globals import get_application_config, get_application_global
from arxiv.base import logging

from fulltext.services import legacy, store, preview, extractor
from fulltext.process import psv
from .domain import Extraction, Status, SupportedFormats, SupportedBuckets

logger = logging.getLogger(__name__)


class NoSuchTask(RuntimeError):
    """A request was made for a non-existant task."""


class TaskCreationFailed(RuntimeError):
    """An extraction task could not be created."""


def get_version() -> str:
    """Get the current version of the extractor."""
    version: str = get_application_config().get('EXTRACTOR_VERSION', '-1.0')
    return version


def is_available(await_result: bool = False) -> bool:
    """Verify that we can start extractions."""
    logger.debug('check connection to task queue')
    try:
        celery_app = get_or_create_worker_app(current_app)
        task = celery_app.send_task('do_nothing')
    except Exception:
        logger.debug('could not connect to task queue')
        return False
    logger.debug('connection to task queue ok')
    if await_result:
        try:
            logger.debug('waiting for task result')
            task.get()    # Blocks until result is available.
        except Exception as e:
            logger.error('Encounted exception while awaiting result: %s', e)
            return False
    return True


def task_id(identifier: str, id_type: str, version: str) -> str:
    """Make a task ID for an extraction."""
    return f"{id_type}::{identifier}::{version}"


def create_task(identifier: str, id_type: str, owner: Optional[str] = None,
                token: Optional[str] = None) -> str:
    """
    Create a new extraction task.

    Parameters
    ----------
    identifier : str
        Unique identifier for the paper being extracted. Usually an arXiv ID.
    pdf_url : str
        The full URL for the PDF from which text will be extracted.
    id_type : str
        Either 'arxiv' or 'submission'.

    Returns
    -------
    str
        The identifier for the created extraction task.

    """
    logger.debug('Create extraction task with %s, %s', identifier, id_type)
    version = get_version()
    storage = store.Storage.current_session()
    try:
        _task_id = task_id(identifier, id_type, version)
        # Create this ahead of time so that the API is immediately consistent,
        # even if it takes a little while for the extraction task to start
        # in the worker.
        storage.store(Extraction(
            identifier=identifier,
            version=version,
            started=datetime.now(UTC),
            bucket=id_type,
            owner=owner,
            task_id=_task_id,
            status=Status.IN_PROGRESS,
        ))
        # Dispatch the extraction task.
        celery_app = get_or_create_worker_app(current_app)
        celery_app.send_task('extract',
                             (identifier, id_type, version),
                             {'token': token},
                             task_id=_task_id)
        logger.info('extract: started processing as %s', _task_id)
    except Exception as e:
        logger.debug(e)
        raise TaskCreationFailed('Failed to create task: %s', e) from e
    return _task_id


def get_task(identifier: str, id_type: str, version: str) -> Extraction:
    """
    Get the status of an extraction task.

    Parameters
    ----------
    identifier : str
        Unique identifier for the paper being extracted. Usually an arXiv ID.
    id_type : str
        Either 'arxiv' or 'submission'.
    version : str
        Extractor version.

    Returns
    -------
    :class:`Extraction`

    """
    _task_id = task_id(identifier, id_type, version)
    result = AsyncResult(_task_id, task_name='extract')
    exception: Optional[str] = None
    owner: Optional[str] = None
    if result.status == 'PENDING':
        raise NoSuchTask('No such task')
    elif result.status in ['SENT', 'STARTED', 'RETRY']:
        _status = Status.IN_PROGRESS
    elif result.status == 'FAILURE':
        _status = Status.FAILED
        exception = str(result.result)
    elif result.status == 'SUCCESS':
        _status = Status.SUCCEEDED
        owner = str(result.result['owner'])
    else:
        raise RuntimeError(f'Unexpected state: {result.status}')
    return Extraction(
        identifier=identifier,
        bucket=id_type,
        task_id=_task_id,
        version=version,
        status=_status,
        exception=exception,
        owner=owner
    )


def _retrieve_pdf(identifier: str, id_type: str, token: Optional[str]) \
        -> IO[bytes]:
    canonical = legacy.CanonicalPDF.current_session()
    previews = preview.PreviewService.current_session()
    pdf_content: IO[bytes]
    if id_type == SupportedBuckets.ARXIV:
        pdf_content = canonical.retrieve(identifier)
    elif id_type == SupportedBuckets.SUBMISSION:
        pdf_content, _ = previews.get(identifier, token)
    else:
        RuntimeError(f'Unsupported identifier: {identifier} ({id_type})')
    return pdf_content


def _store_pdf_in_workdir(identifier: str, id_type: str, content: IO[bytes]) \
        -> str:
    workdir: str = get_application_config()['WORKDIR']
    prefix = f'{id_type}-{identifier}'

    # The prefix might have a forward slash in it, so we want to make sure that
    # we have all of the necessary directories.
    containing, _ = os.path.split(os.path.join(workdir, prefix))
    if not os.path.exists(containing):
        os.makedirs(containing)
    _, prefix = os.path.join(workdir, prefix).split(containing, 1)
    prefix = prefix[1:] if prefix.startswith('/') else prefix

    _, pdf_path = tempfile.mkstemp(dir=containing, prefix=prefix, suffix='.pdf')
    with open(pdf_path, 'wb') as f:
        f.write(content.read())
    os.chmod(pdf_path, 0o644)
    return pdf_path


def extract(identifier: str, id_type: str, version: str,
            owner: Optional[str] = None,
            token: Optional[str] = None) -> Dict[str, str]:
    """Perform text extraction for a single arXiv document."""
    logger.debug('Perform extraction for %s in bucket %s with version %s',
                 identifier, id_type, version)
    storage = store.Storage.current_session()

    # This assumes we have a metadata record on disk already.
    extraction = storage.retrieve(identifier, version, bucket=id_type,
                                  meta_only=True)

    pdf_path: Optional[str] = None
    try:
        pdf_content = _retrieve_pdf(identifier, id_type, token)
        pdf_path = _store_pdf_in_workdir(identifier, id_type, pdf_content)
        content = extractor.do_extraction(pdf_path)

    except Exception as e:
        logger.error('Failed to process %s: %s', identifier, e)
        storage.store(extraction.copy(status=Status.FAILED,
                                      ended=datetime.now(UTC),
                                      exception=str(e)))
        raise e
    finally:
        if pdf_path is not None:
            os.remove(pdf_path)    # Cleanup.

    extraction = extraction.copy(status=Status.SUCCEEDED,
                                 ended=datetime.now(UTC),
                                 content=content)
    storage.store(extraction, 'plain')
    extraction = extraction.copy(content=psv.normalize_text_psv(content))
    storage.store(extraction, 'psv')
    result = extraction.to_dict()
    result.pop('content')
    return result


@after_task_publish.connect
def update_sent_state(sender: Optional[str] = None,
                      headers: Optional[Dict[str, str]] = None,
                      body: Any = None, **kwargs: Any) -> None:
    """Set state to SENT, so that we can tell whether a task exists."""
    celery_app = get_or_create_worker_app(current_app)
    task = celery_app.tasks.get(sender)
    backend = task.backend if task else celery_app.backend
    if headers is not None:
        backend.store_result(headers['id'], None, "SENT")


def do_nothing() -> None:
    """Dummy task used to check the connection to the queue."""
    return


def create_worker_app(app: Flask) -> Celery:
    """
    Initialize the worker application.

    Returns
    -------
    :class:`celery.Celery`

    """
    result_backend = app.config['CELERY_RESULT_BACKEND']
    broker = app.config['CELERY_BROKER_URL']
    celery_app = Celery('fulltext',
                        results=result_backend,
                        backend=result_backend,
                        result_backend=result_backend,
                        broker=broker)

    celery_app.conf.queue_name_prefix = app.config['CELERY_QUEUE_NAME_PREFIX']
    celery_app.conf.task_default_queue = app.config['CELERY_TASK_DEFAULT_QUEUE']
    celery_app.conf.prefetch_multiplier = app.config['CELERY_PREFETCH_MULTIPLIER']
    celery_app.conf.task_acks_late = app.config['CELERY_TASK_ACKS_LATE']
    celery_app.conf.backend = result_backend
    celery_app.conf.result_extended = app.config['CELERY_RESULT_EXTENDED']

    celery_app.task(extract, name='extract')
    celery_app.task(do_nothing, name='do_nothing')
    return celery_app


def get_or_create_worker_app(app: Flask) -> Celery:
    """
    Get the current worker app, or create one.

    Uses the Flask application global to keep track of the worker app.
    """
    g = get_application_global()
    if not g:
        return create_worker_app(app)
    if 'worker' not in g:
        g.worker = create_worker_app(app)
    return g.worker