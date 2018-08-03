"""Provides asynchronous task for fulltext extraction."""

import os
from typing import Tuple
from datetime import datetime
import shutil
import subprocess
import shlex
import tempfile
from flask import current_app
from celery.result import AsyncResult
from fulltext.celery import celery_app
from celery.signals import after_task_publish

from arxiv.base.globals import get_application_config, get_application_global
from arxiv.base import logging

from fulltext.services import store, pdf
from fulltext.process import psv

logger = logging.getLogger(__name__)


@celery_app.task
def extract_fulltext(document_id: str, pdf_url: str, id_type: str = 'arxiv') \
        -> None:
    """Perform fulltext extraction for a single arXiv document."""
    logger.info('Retrieving PDF for %s' % document_id)
    start_time = datetime.now()
    try:
        # Retrieve PDF from arXiv central document store.
        pdf_path = pdf.retrieve(pdf_url, document_id)
        if pdf_path is None:
            raise RuntimeError('%s: no PDF available' % document_id)
        logger.info('%s: retrieved PDF' % document_id)

        logger.info('Attempting text extraction for %s' % document_id)
        content = do_extraction(pdf_path)
        logger.info('Text extraction for %s succeeded with %i chars' %
                    (document_id, len(content)))
        try:
            store.store(document_id, content, bucket=id_type)
        except RuntimeError as e:   # TODO: flesh out exception states.
            raise
        duration = (start_time - datetime.now()).microseconds
        logger.info(f'Finished extraction for {document_id} in {duration} ms')

        psv_content = psv.normalize_text_psv(content)
        try:
            store.store(document_id, psv_content, content_format='psv',
                        bucket=id_type)
        except RuntimeError as e:   # TODO: flesh out exception states.
            raise
        logger.info(f'Stored PSV normalized content for {document_id}')

        os.remove(pdf_path)    # Cleanup.

    except Exception as e:
        logger.error('Failed to process %s: %s' % (document_id, e))
        content = {'exception': str(e), 'content': None}
        store.store(document_id, content, is_placeholder=True)
        raise e
    return {'paper_id': document_id, 'id_type': id_type}


@after_task_publish.connect
def update_sent_state(sender=None, headers=None, body=None, **kwargs):
    """Set state to SENT, so that we can tell whether a task exists."""
    task = celery_app.tasks.get(sender)
    backend = task.backend if task else celery_app.backend
    backend.store_result(headers['id'], None, "SENT")


def do_extraction(filename: str, cleanup: bool = False,
                  image: str = 'arxiv/fulltext') -> str:
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
    workdir = current_app.config.get('WORKDIR', '/tmp/pdfs')
    fldr, name = os.path.split(filename)
    stub, ext = os.path.splitext(os.path.basename(filename))
    pdfpath = os.path.join(workdir, name)
    shutil.copyfile(filename, pdfpath)
    logger.info('Copied %s to %s' % (filename, pdfpath))
    logger.info(str(os.listdir(workdir)))

    try:
        run_docker(image, [[workdir, '/pdfs']],
                   args='/pdfs/%s' % name)
    except subprocess.CalledProcessError as e:
        raise RuntimeError('Fulltext failed: %s' % filename) from e

    out = os.path.join(workdir, '{}.txt'.format(stub))
    os.remove(pdfpath)
    if not os.path.exists(out):
        raise FileNotFoundError('%s not found, expected output' % out)
    with open(out, 'rb') as f:
        content = f.read().decode('utf-8')
    os.remove(out.replace('.txt', '.pdf2txt'))
    os.remove(out)    # Cleanup.
    return content


def run_docker(image: str, volumes: list = [], ports: list = [],
               args: str = '', daemon: bool = False) -> Tuple[str, str]:
    """
    Run a generic docker image.

    In our uses, we wish to set the userid to that of running process (getuid)
    by default. Additionally, we do not expose any ports for running services
    making this a rather simple function.

    Parameters
    ----------
    image : str
        Name of the docker image in the format 'repository/name:tag'

    volumes : list of tuple of str
        List of volumes to mount in the format [host_dir, container_dir].

    args : str
        Arguments to the image's run cmd (set by Dockerfile CMD)

    daemon : boolean
        If True, launches the task to be run forever
    """
    # we are only running strings formatted by us, so let's build the command
    # then split it so that it can be run by subprocess
    opt_user = '-u {}'.format(os.getuid())
    opt_volumes = ' '.join(['-v {}:{}'.format(hd, cd) for hd, cd in volumes])
    opt_ports = ' '.join(['-p {}:{}'.format(hp, cp) for hp, cp in ports])
    cmd = 'docker run --rm {} {} {} {} {}'.format(
        opt_user, opt_ports, opt_volumes, image, args
    )
    result = subprocess.run(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    if result.returncode:
        _cmd = ' '.join(cmd)
        logger.error(f"Docker image call '{_cmd}' exited {result.returncode}")
        logger.error(f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}")
        result.check_returncode()

    return result
