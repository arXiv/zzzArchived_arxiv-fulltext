"""Service layer integration for fulltext extractor."""

import os
import shutil
import subprocess
import shlex
import tempfile
from flask import _app_ctx_stack as stack

from fulltext import logging

logger = logging.getLogger(__name__)


class FullTextSession(object):
    """Represents a configured FullText session."""

    def __init__(self, image: str) -> None:
        """
        Set the Docker image for FullText.

        Parameters
        ----------
        image : str
        """
        self.image = image
        try:
            # TODO: docker pull to verify image is available.
            pass
        except Exception as e:
            raise IOError('Failed to pull FullText image %s: %s' %
                          (self.image, e)) from e

    def extract_fulltext(self, filename: str, cleanup: bool=False):
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
        fldr, name = os.path.split(filename)
        stub, ext = os.path.splitext(os.path.basename(filename))
        tmpdir = tempfile.mkdtemp()

        # tmppdf = os.path.join(tmpdir, name)
        pdfpath = os.path.join('/tmp/pdfs', name)
        shutil.copyfile(filename, pdfpath)
        logger.info('Copied %s to %s' % (filename, pdfpath))
        logger.info(str(os.listdir('/tmp/pdfs')))

        try:
            run_docker(self.image, [['/tmp/pdfs', '/tmp/pdfs']],
                       args='/scripts/extract.sh /tmp/pdfs/%s' % name,
                       aws_login=".amazonaws.com" in self.image)
        except subprocess.CalledProcessError as e:
            raise RuntimeError('Fulltext failed: %s' % filename) from e

        out = os.path.join('/tmp/pdfs', '{}.txt'.format(stub))
        if not os.path.exists(out):
            raise FileNotFoundError('%s not found, expected output' % out)
        return out


class FullText(object):
    """FullText integration from fulltext worker application."""

    def __init__(self, app=None):
        """Set and configure the current application instance, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        app.config.setdefault('FULLTEXT_DOCKER_IMAGE', 'arxiv/fulltext')

    def get_session(self) -> FullTextSession:
        """Generate a new configured :class:`.FullTextSession`."""
        try:
            image = self.app.config['FULLTEXT_DOCKER_IMAGE']
        except (RuntimeError, AttributeError) as e:   # No application context.
            image = os.environ.get('FULLTEXT_DOCKER_IMAGE',
                                   'arxiv/fulltext')
        return FullTextSession(image)

    @property
    def session(self):
        """Get or creates a :class:`.FullTextSession` for the current app."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'fulltext_extractor'):
                ctx.fulltext_extractor = self.get_session()
            return ctx.fulltext_extractor
        return self.get_session()     # No application context.


def run_docker(image: str, volumes: list = [], ports: list = [],
               args: str = '', daemon: bool = False,
               aws_login: bool=True) -> (str, str):
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
    if aws_login:
        login = "$(aws ecr get-login --no-include-email --region us-east-1)"
        cmd = "%s && %s" % (login, cmd)

    result = subprocess.run(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    if result.returncode:
        logger.error(
            "Docker image call '{}' returned error {}".format(
                ' '.join(cmd), result.returncode
            )
        )
        logger.error(
            "STDOUT: {}\nSTDERR: {}".format(result.stdout, result.stderr)
        )
        result.check_returncode()

    return result


extractor = FullText()
