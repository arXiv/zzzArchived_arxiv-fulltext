"""Service integration for central arXiv document store."""

import requests
import os
import time
from datetime import datetime, timedelta
import json
from urllib.parse import urljoin
from arxiv.base import logging
from fulltext.context import get_application_config, get_application_global

logger = logging.getLogger(__name__)


class RequestExtractionSession(object):
    """Provides an interface to the reference extraction service."""

    def __init__(self, endpoint: str) -> None:
        """Set the endpoint for Refextract service."""
        self.endpoint = endpoint
        self._session = requests.Session()
        self._adapter = requests.adapters.HTTPAdapter(max_retries=2)
        self._session.mount('http://', self._adapter)

    def status(self):
        """Get the status of the extraction service."""
        try:
            response = self._session.get(urljoin(self.endpoint,
                                                 '/fulltext/status'))
        except IOError:
            return False
        if not response.ok:
            return False
        return True

    def extract(self, document_id: str, pdf_url: str) -> dict:
        """
        Request fulltext extraction.

        Parameters
        ----------
        document_id : str
        pdf_url : str

        Returns
        -------
        dict
        """
        payload = {'document_id': document_id, 'url': pdf_url}
        response = self._session.post(urljoin(self.endpoint, '/fulltext'),
                                      data=json.dumps(payload))
        if not response.ok:
            raise IOError('Extraction request failed with status %i: %s' %
                          (response.status_code, response.content))

        target_url = urljoin(self.endpoint, '/fulltext/%s' % document_id)
        try:
            status_url = response.headers['Location']
        except KeyError:
            status_url = response.url
        if status_url == target_url:    # Extraction already performed.
            return response.json()

        failed = 0
        start = datetime.now()    # If this runs too long, we'll abort.
        while not response.url.startswith(target_url):
            if failed > 8:    # TODO: make this configurable?
                msg = '%s: cannot get extraction state: %s, %s' % \
                      (document_id, response.status_code, response.content)
                logger.error(msg)
                raise IOError(msg)

            if datetime.now() - start > timedelta(seconds=300):
                msg = '%s: extraction did not complete within five minutes' % \
                      document_id
                logger.error(msg)
                raise IOError(msg)

            time.sleep(2 + failed * 2)    # Back off.
            try:
                # Might be a 200-series response with Location header.
                target = response.headers.get('Location', response.url)
                response = self._session.get(target)
            except Exception as e:
                msg = '%s: cannot get extraction state: %s' % (document_id, e)
                logger.error(msg)
                failed += 1

            if not response.ok:
                failed += 1

        return response.json()


def get_session(app: object = None) -> RequestExtractionSession:
    """Get a new extraction session."""
    endpoint = get_application_config(app).get('EXTRACTION_ENDPOINT')
    if not endpoint:
        raise RuntimeError('EXTRACTION_ENDPOINT not set')
    return RequestExtractionSession(endpoint)


def current_session():
    """Get/create :class:`.RequestExtractionSession` for this context."""
    g = get_application_global()
    if g is None:
        return get_session()
    if 'extract' not in g:
        g.extract = get_session()
    return g.extract


def extract(document_id: str, pdf_url: str) -> dict:
    """Extract text using the current session."""
    return current_session().extract(document_id, pdf_url)
