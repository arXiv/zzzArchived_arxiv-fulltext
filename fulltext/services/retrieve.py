"""Service integration for central arXiv document store."""

import requests
import os
from fulltext import logging
from urllib.parse import urlparse
# See http://flask.pocoo.org/docs/0.12/extensiondev/
from flask import _app_ctx_stack as stack
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


class RetrievePDF(object):
    """PDF retrieval from central document store."""

    def __init__(self, app=None):
        """Set and configure application, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        app.config.set_default('SOURCE_WHITELIST',
                               'arxiv.org,export.arxiv.org')

    def get_session(self) -> None:
        """Create a new :class:`.RetrievePDFSession`."""
        try:
            # endpoint = self.app.config['PDF_ENDPOINT']
            whitelist = self.app.config['SOURCE_WHITELIST'].split(',')
        except (RuntimeError, AttributeError) as e:   # No application context.
            # endpoint = os.environ.get('PDF_ENDPOINT')
            whitelist = os.environ.get('SOURCE_WHITELIST',
                                       'arxiv.org,export.arxiv.org').split(',')
        return RetrievePDFSession(whitelist)

    @property
    def session(self):
        """Get or create a :class:`.RetrievePDFSession` for this context."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'retrieve'):
                ctx.retrieve = self.get_session()
            return ctx.retrieve
        return self.get_session()     # No application context.


retrievePDF = RetrievePDF()
