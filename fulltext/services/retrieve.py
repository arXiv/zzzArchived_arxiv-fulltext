"""Service integration for central arXiv document store."""

import requests
import os
from arxiv.base import logging
from urllib.parse import urlparse
from fulltext.context import get_application_config, get_application_global
import tempfile

logger = logging.getLogger(__name__)


class RetrievePDFSession(object):
    """Provides an interface to get PDF."""

    def __init__(self, whitelist: list) -> None:
        """Set the endpoint for Refextract service."""
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

    def retrieve(self, target: str, document_id: str) -> str:
        """
        Retrieve PDFs of published papers from the core arXiv document store.

        Parameters
        ----------
        target : str
        document_id : str

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
        # target = '%s/pdf/%s.pdf' % (self.endpoint, document_id)
        if not self.is_valid_url(target):
            raise ValueError('URL not allowed: %s' % target)

        pdf_response = requests.get(target)
        if pdf_response.status_code == requests.codes.NOT_FOUND:
            logger.info('Could not retrieve PDF for %s' % document_id)
            return None
        elif pdf_response.status_code != requests.codes.ok:
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


def is_valid_url(url: str) -> bool:
    """Evaluate whether or not a URL is acceptible for retrieval."""
    return current_session().is_valid_url(url)


def retrieve(target: str, document_id: str) -> str:
    """Retrieve a PDF of a paper from the core arXiv document store."""
    return current_session().retrieve(target, document_id)
