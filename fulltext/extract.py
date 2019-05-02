"""Provides asynchronous task for fulltext extraction."""

import os
from typing import Tuple, Optional, Dict
from datetime import datetime
from pytz import UTC
from json import dumps
import shutil
import subprocess
import shlex
import tempfile

from flask import current_app
from celery.result import AsyncResult
from celery.signals import after_task_publish
import docker
from docker.errors import ContainerError, APIError

from arxiv.base.globals import get_application_config, get_application_global
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
    return get_application_config().get('EXTRACTOR_VERSION', '-1.0')


def task_id(identifier: str, id_type: str, version: str) -> str:
    return f"{id_type}::{identifier}::{version}"


def create_extraction_task(identifier: str, id_type: str,
                           owner: Optional[str] = None,
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
    try:
        _task_id = task_id(identifier, id_type, version)
        # Create this ahead of time so that the API is immediately consistent,
        # even if it takes a little while for the extraction task to start
        # in the worker.
        store.Storage.store(Extraction(
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
        logger.info('extract: started processing as %s' % _task_id)
    except Exception as e:
        logger.debug(e)
        raise TaskCreationFailed('Failed to create task: %s', e) from e
    return _task_id


def get_extraction_task(identifier: str, id_type: str,
                        version: Optional[str] = None) -> Extraction:
    """
    Get the status of an extraction task.

    Parameters
    ----------
    identifier : str
        Unique identifier for the paper being extracted. Usually an arXiv ID.
    id_type : str
        Either 'arxiv' or 'submission'.
    version : str
        Extractor version (optional). Will use the current version if not
        provided.

    Returns
    -------
    :class:`Extraction`

    """
    _task_id = task_id(identifier, id_type, version)
    result = extract.AsyncResult(_task_id)
    data = {
        'task_id': task_id,
        'version': version,
        'identifer': identifier,
        'bucket': id_type
    }
    if result.status == 'PENDING':
        raise NoSuchTask('No such task')
    elif result.status in ['SENT', 'STARTED', 'RETRY']:
        data['status'] = Extraction.Status.IN_PROGRESS
    elif result.status == 'FAILURE':
        data['status'] = Extraction.Status.FAILED
        data['result']: str = result.result
    elif result.status == 'SUCCESS':
        data['status'] = Extraction.Status.SUCCEEDED
        _result: Dict[str, str] = result.result
        data['owner'] = _result['owner']
    return Extraction(**data)


def extraction_task_exists(identifier: str, id_type: str,
                           version: Optional[str] = None) -> bool:
    """
    Check whether an extraction task exists.

    Parameters
    ----------
    identifier : str
        Unique identifier for the paper being extracted. Usually an arXiv ID.
    id_type : str
        Either 'arxiv' or 'submission'.
    version : str
        Extractor version (optional). Will use the current version if not
        provided.

    Returns
    -------
    bool

    """
    logger.debug('task exists? %s, %s, %s', identifier, id_type, version)
    result = extract.AsyncResult(task_id(identifier, id_type, version))
    return result.status != 'PENDING'   # 'PENDING' => non-existant.


@celery_app.task
def extract(identifier: str, id_type: str, version: str,
            owner: Optional[str] = None,
            token: Optional[str] = None) -> Dict[str, str]:
    """Perform text extraction for a single arXiv document."""
    logger.debug('Perform extraction for %s in bucket %s with version %s',
                 identifier, id_type, version)
    try:
        if id_type == SupportedBuckets.ARXIV:
            pdf_path = pdf.CanonicalPDF.retrieve(identifier)
        elif id_type == SupportedBuckets.SUBMISSION:
            pdf_path, owner = compiler.Compiler.retrieve(identifier, token)
        else:
            RuntimeError('Unsupported identifier')
        extraction = store.Storage.retrieve(identifier, version,
                                            bucket=id_type,
                                            meta_only=True)
        content = do_extraction(pdf_path)
        assert content is not None
    except Exception as e:
        logger.error('Failed to process %s: %s' % (identifier, e))
        store.Storage.store(extraction.copy(status=Extraction.Status.FAILED,
                            ended=datetime.now(UTC),
                            exception=str(e)))
        raise e
    finally:
        os.remove(pdf_path)    # Cleanup.

    extraction = extraction.copy(status=Extraction.Status.SUCCEEDED,
                                 ended=datetime.now(UTC),
                                 content=content)
    store.Storage.store(extraction, 'plain')
    extraction = extraction.copy(content=psv.normalize_text_psv(content))
    store.Storage.store(extraction, 'psv')
    result = extraction.to_dict()
    result.pop('content')
    return result


@after_task_publish.connect
def update_sent_state(sender=None, headers=None, body=None, **kwargs):
    """Set state to SENT, so that we can tell whether a task exists."""
    task = celery_app.tasks.get(sender)
    backend = task.backend if task else celery_app.backend
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
    logger.info('Attempting text extraction for %s' % filename)
    start_time = datetime.now()

    # This is the path in this container/env where PDFs are stored.
    workdir = current_app.config['WORKDIR']
    # This is the path on the Docker host that should be mapped into the
    # extractor container at /pdf. This is the same volume that should be
    # mounted at ``workdir`` in this container/env.
    mountdir = current_app.config['MOUNTDIR']
    # The result is something like:
    #                  | <--- /{workdir} (this container)
    # /{mountdir} (host) |
    #                  | <--- /pdfs (extractor container)

    if image is None:
        image_name = current_app.config['EXTRACTOR_IMAGE']
        image_tag = current_app.config['EXTRACTOR_VERSION']
        image = f'{image_name}:{image_tag}'

    docker_host = current_app.config['DOCKER_HOST']

    fldr, name = os.path.split(filename)
    stub, ext = os.path.splitext(os.path.basename(filename))
    pdfpath = os.path.join(workdir, name)
    shutil.copyfile(filename, pdfpath)
    logger.info('Copied %s to %s' % (filename, pdfpath))

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
    logger.info(f'Finished extraction for {filename} in {duration} ms')
    return content
