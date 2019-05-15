"""Service integration for PDF retrieval."""

import os
import tempfile
import time

import requests

from arxiv.base import logging
from arxiv.integration.api import status, service


class InvalidURL(ValueError):
    """A request was made for a URL that is not allowed."""


class DoesNotExist(RuntimeError):
    """A request was made for a non-existant PDF."""


logger = logging.getLogger(__name__)


class CanonicalPDF(service.HTTPIntegration):
    """Provides an interface to get PDFs."""

    class Meta:
        """Configuration for :class:`CanonicalPDF`."""

        service_name = "canonical"

    def is_available(self) -> bool:
        """Determine whether canonical PDFs are available."""
        response = self._session.head(self._path(f'/'), allow_redirects=True)
        return bool(response.status_code == status.OK)

    def exists(self, identifier: str) -> bool:
        """
        Determine whether or not a target URL is available (HEAD request).

        Parameters
        ----------
        identifier : str
            arXiv identifier for which a PDF is required.

        Returns
        -------
        bool

        """
        r = self._session.head(self._path(f'/pdf/{identifier}'),
                               allow_redirects=True)
        if r.status_code == status.OK:
            return True
        if r.status_code == status.NOT_FOUND:
            return False
        raise IOError(f'Unexpected response status code: {r.status_code}')

    def retrieve(self, identifier: str, sleep: int = 5) -> str:
        """
        Retrieve PDFs of published papers from the core arXiv document store.

        Parameters
        ----------
        identifier : str
            arXiv identifier for which a PDF is required.

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
        target = self._path(f'/pdf/{identifier}')
        pdf_response = self._session.get(target)
        if pdf_response.status_code == status.NOT_FOUND:
            logger.info('Could not retrieve PDF for %s', identifier)
            raise DoesNotExist('No such resource')
        if pdf_response.status_code != status.OK:
            raise IOError('%s: unexpected status for PDF: %i' %
                          (identifier, pdf_response.status_code))

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
                if pdf_response.status_code != status.OK:
                    raise IOError('%s: unexpected status for PDF: %i' %
                                  (identifier, pdf_response.status_code))

        _, pdf_path = tempfile.mkstemp(prefix=identifier, suffix='.pdf')
        with open(pdf_path, 'wb') as f:
            f.write(pdf_response.content)
        os.chmod(pdf_path, 0o644)
        return pdf_path
