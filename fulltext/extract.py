"""Provides asynchronous task for fulltext extraction."""

import os
from typing import Tuple, Optional, Dict, Any
from datetime import datetime
from pytz import UTC
import shutil

from flask import current_app
from celery.result import AsyncResult
from celery.signals import after_task_publish

import docker
from docker.errors import ContainerError, APIError

from arxiv.base.globals import get_application_config
from arxiv.base import logging

from fulltext.celery import celery_app
from fulltext.services import store, pdf, compiler
from fulltext.process import psv
from .domain import Extraction, SupportedFormats, SupportedBuckets

logger = logging.getLogger(__name__)


class NoSuchTask(RuntimeError):
    """A request was made for a non-existant task."""


class TaskCreationFailed(RuntimeError):
    """An extraction task could not be created."""


def get_version() -> str:
    """Get the current version of the extractor."""
    version: str = get_application_config().get('EXTRACTOR_VERSION', '-1.0')
    return version


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
            status=Extraction.Status.IN_PROGRESS,
        ))
        # Dispatch the extraction task.
        extract.apply_async((identifier, id_type, version), {'token': token},
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
    result = extract.AsyncResult(_task_id)
    exception: Optional[str] = None
    owner: Optional[str] = None
    if result.status == 'PENDING':
        raise NoSuchTask('No such task')
    elif result.status in ['SENT', 'STARTED', 'RETRY']:
        _status = Extraction.Status.IN_PROGRESS
    elif result.status == 'FAILURE':
        _status = Extraction.Status.FAILED
        exception = str(result.result)
    elif result.status == 'SUCCESS':
        _status = Extraction.Status.SUCCEEDED
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


@celery_app.task
def extract(identifier: str, id_type: str, version: str,
            owner: Optional[str] = None,
            token: Optional[str] = None) -> Dict[str, str]:
    """Perform text extraction for a single arXiv document."""
    logger.debug('Perform extraction for %s in bucket %s with version %s',
                 identifier, id_type, version)

    canonical = pdf.CanonicalPDF.current_session()
    storage = store.Storage.current_session()
    compilations = compiler.Compiler.current_session()

    try:
        if id_type == SupportedBuckets.ARXIV:
            pdf_path = canonical.retrieve(identifier)
        elif id_type == SupportedBuckets.SUBMISSION:
            pdf_path, owner = compilations.retrieve(identifier, token)
        else:
            RuntimeError(f'Unsupported identifier: {identifier} ({id_type})')
        extraction = storage.retrieve(identifier, version, bucket=id_type,
                                      meta_only=True)
        content = do_extraction(pdf_path)
        assert content is not None
    except Exception as e:
        logger.error('Failed to process %s: %s', identifier, e)
        storage.store(extraction.copy(status=Extraction.Status.FAILED,
                                      ended=datetime.now(UTC),
                                      exception=str(e)))
        raise e
    finally:
        os.remove(pdf_path)    # Cleanup.

    extraction = extraction.copy(status=Extraction.Status.SUCCEEDED,
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
    task = celery_app.tasks.get(sender)
    backend = task.backend if task else celery_app.backend
    if headers is not None:
        backend.store_result(headers['id'], None, "SENT")


def do_extraction(filename: str, cleanup: bool = False,
                  image: Optional[str] = None) -> str:
    """
    Extract fulltext from the PDF represented by ``filehandle``.

    Parameters
    ----------
    filename : str

    Returns
    -------
    str
        Raw XML response from FullText.

    """
    logger.info('Attempting text extraction for %s', filename)
    start_time = datetime.now()

    # This is the path in this container/env where PDFs are stored.
    workdir = current_app.config['WORKDIR']
    # This is the path on the Docker host that should be mapped into the
    # extractor container at /pdf. This is the same volume that should be
    # mounted at ``workdir`` in this container/env.
    mountdir = current_app.config['MOUNTDIR']
    # The result is something like:
    #
    #                       | <-- {workdir} (worker)
    # [working volume] <--- |
    #                       | <-- {mountdir} (dind) <-- /pdfs (extractor)
    #

    if image is None:
        image_name = current_app.config['EXTRACTOR_IMAGE']
        image_tag = current_app.config['EXTRACTOR_VERSION']
        image = f'{image_name}:{image_tag}'

    docker_host = current_app.config['DOCKER_HOST']

    fldr, name = os.path.split(filename)
    stub, ext = os.path.splitext(os.path.basename(filename))
    pdfpath = os.path.join(workdir, name)
    shutil.copyfile(filename, pdfpath)
    logger.info('Copied %s to %s', filename, pdfpath)

    try:
        client = docker.DockerClient(docker_host)
        client.images.pull(image_name, image_tag)
        volumes = {mountdir: {'bind': '/pdfs', 'mode': 'rw'}}
        client.containers.run(image, f'/pdfs/{name}', volumes=volumes)
    except (ContainerError, APIError) as e:
        raise RuntimeError('Fulltext failed: %s' % filename) from e

    out = os.path.join(workdir, '{}.txt'.format(stub))
    os.remove(pdfpath)
    if not os.path.exists(out):
        raise FileNotFoundError('%s not found, expected output' % out)
    with open(out, 'rb') as f:
        content = f.read().decode('utf-8')
    os.remove(out.replace('.txt', '.pdf2txt'))
    os.remove(out)    # Cleanup.
    duration = (start_time - datetime.now()).microseconds
    logger.info(f'Finished extraction for %s in %s ms', filename, duration)
    return content
