"""Service integration for PDF retrieval."""

from typing import List
from functools import wraps
import requests
import os
import tempfile
import time
from urllib.parse import urlparse

from requests.packages.urllib3.util.retry import Retry

from arxiv import status
from arxiv.base import logging
from arxiv.base.globals import get_application_config, get_application_global


class InvalidURL(ValueError):
    """A request was made for a URL that is not allowed."""


class DoesNotExist(RuntimeError):
    """A request was made for a non-existant PDF."""


logger = logging.getLogger(__name__)


class RetrievePDFSession(object):
    """Provides an interface to get PDFs."""

    def __init__(self, whitelist: List[str], scheme: str = 'https',
                 verify_cert: bool = True, headers: dict = {}) -> None:
        """
        Initialize an HTTP session.

        Parameters
        ----------
        endpoint : str
            Service endpoint for PDF retrieval.
        scheme : str
            Default: ``https``.
        verify_cert : bool
            Whether or not SSL certificate verification should enforced.
        headers : dict
            Headers to be included on all requests.

        """
        self._session = requests.Session()
        self._verify_cert = verify_cert
        self._retry = Retry(  # type: ignore
            total=10,
            read=10,
            connect=10,
            status=10,
            backoff_factor=0.5
        )
        self._adapter = requests.adapters.HTTPAdapter(max_retries=self._retry)
        self._session.mount(f'{scheme}://', self._adapter)
        self._session.headers.update(headers)
        self._whitelist = whitelist

    def is_valid_url(self, url: str) -> bool:
        """
        Evaluate whether or not a URL is acceptible for retrieval.

        Parameters
        ----------
        url : str
            Location of a document.

        Returns
        -------
        bool
        """
        o = urlparse(url)
        if o.netloc not in self._whitelist:
            return False
        return True

    def exists(self, target: str) -> bool:
        """
        Determine whether or not a target URL is available (HEAD request).

        Parameters
        ----------
        target : str
            The full URL for the PDF.

        Returns
        -------
        bool

        """
        if not self.is_valid_url(target):
            raise InvalidURL('URL not allowed: %s' % target)
        r = self._session.head(target, allow_redirects=True)
        if r.status_code == status.HTTP_200_OK:
            return True
        elif r.status_code == status.HTTP_404_NOT_FOUND:
            return False
        raise IOError(f'Unexpected response status code: {r.status_code}')

    def retrieve(self, target: str, document_id: str, sleep: int = 5) -> str:
        """
        Retrieve PDFs of published papers from the core arXiv document store.

        Parameters
        ----------
        target : str
            The full URL for the PDF.
        document_id : str
            The identifier associated with the PDF.

        Returns
        -------
        str
            Path to (temporary) PDF.

        Raises
        ------
        ValueError
            If a disallowed or otherwise invalid URL is passed.
        IOError
            When there is a problem retrieving the resource at ``target``.
        """
        if not self.is_valid_url(target):
            raise InvalidURL('URL not allowed: %s' % target)

        pdf_response = self._session.get(target)
        if pdf_response.status_code == status.HTTP_404_NOT_FOUND:
            logger.info('Could not retrieve PDF for %s' % document_id)
            raise DoesNotExist('No such resource')
        elif pdf_response.status_code != requests.codes.ok:
            raise IOError('%s: unexpected status for PDF: %i' %
                          (document_id, pdf_response.status_code))

        # Classic PDF route will return 200 even if PDF is not yet generated.
        # But at least it will be honest about the Content-Type. If we don't
        # get a PDF back, we should wait and try again.
        if pdf_response.headers['Content-Type'] != 'application/pdf':
            retries = 5
            while pdf_response.headers['Content-Type'] != 'application/pdf':
                if retries < 1:
                    raise IOError('Could not retrieve PDF; giving up')
                logger.info('Got HTML instead of PDF; retrying (%i remaining)',
                            retries)
                time.sleep(sleep)
                retries -= 1
                pdf_response = self._session.get(target)
                if pdf_response.status_code != requests.codes.ok:
                    raise IOError('%s: unexpected status for PDF: %i' %
                                  (document_id, pdf_response.status_code))

        _, pdf_path = tempfile.mkstemp(prefix=document_id, suffix='.pdf')
        with open(pdf_path, 'wb') as f:
            f.write(pdf_response.content)
        os.chmod(pdf_path, 0o775)
        return pdf_path


def init_app(app: object=None) -> None:
    """Configure an application instance."""
    config = get_application_config(app)
    config.setdefault('SOURCE_WHITELIST', 'arxiv.org,export.arxiv.org')


def get_session(app: object=None) -> RetrievePDFSession:
    """Create a new :class:`.RetrievePDFSession`."""
    config = get_application_config()
    whitelist = config.get('SOURCE_WHITELIST', 'arxiv.org,export.arxiv.org')
    return RetrievePDFSession(whitelist.split(','))


def current_session():
    """Get/create :class:`.RetrievePDFSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'retrieve' not in g:
        g.retrieve = get_session()
    return g.retrieve


@wraps(RetrievePDFSession.is_valid_url)
def is_valid_url(url: str) -> bool:
    """Evaluate whether or not a URL is acceptible for retrieval."""
    return current_session().is_valid_url(url)


@wraps(RetrievePDFSession.retrieve)
def retrieve(target: str, document_id: str, sleep: int = 5) -> str:
    """Retrieve a PDF of a paper from the core arXiv document store."""
    return current_session().retrieve(target, document_id, sleep=sleep)


@wraps(RetrievePDFSession.exists)
def exists(target: str) -> str:
    """Determine whether or not a target URL is available (HEAD request)."""
    return current_session().exists(target)
