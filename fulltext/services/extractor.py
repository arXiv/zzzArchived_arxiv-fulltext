"""Service integration for central arXiv document store."""

import requests
import os
import time
from datetime import datetime, timedelta
import json
from fulltext import logging
# See http://flask.pocoo.org/docs/0.12/extensiondev/
from flask import _app_ctx_stack as stack
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class RequestExtractionSession(object):
    """Provides an interface to the reference extraction service."""

    def __init__(self, endpoint: str) -> None:
        """Set the endpoint for Refextract service."""
        self.endpoint = endpoint
        response = requests.get(urljoin(self.endpoint, '/status'))
        if not response.ok:
            raise IOError('Extraction endpoint not available: %s' %
                          response.content)

    def extract(self, document_id: str, pdf_url: str) -> dict:
        """
        Request reference extraction.

        Parameters
        ----------
        document_id : str
        pdf_url : str

        Returns
        -------
        dict
        """
        payload = {'document_id': document_id, 'url': pdf_url}
        response = requests.post(urljoin(self.endpoint, '/fulltext'),
                                 data=json.dumps(payload))
        if not response.ok:
            raise IOError('Extraction request failed with status %i: %s' %
                          (response.status_code, response.content))

        target_url = urljoin(self.endpoint, '/fulltext/%s' % document_id)

        failed = 0
        start = datetime.now()    # If this runs too long, we'll abort.
        while not response.url.startswith(target_url):
            if failed > 2:    # TODO: make this configurable?
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
                response = requests.get(response.url)
            except Exception as e:
                msg = '%s: cannot get extraction state: %s' % (document_id, e)
                logger.error(msg)
                raise IOError(msg) from e

            if not response.ok:
                failed += 1

        return response.json()


class RequestExtraction(object):
    """Extraction service integration."""

    def __init__(self, app=None):
        """Set and configure application, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        pass

    def get_session(self) -> None:
        """Create a new :class:`.RequestExtractionSession`."""
        try:
            endpoint = self.app.config['EXTRACTION_ENDPOINT']
        except (RuntimeError, AttributeError) as e:   # No application context.
            endpoint = os.environ.get('EXTRACTION_ENDPOINT')
        return RequestExtractionSession(endpoint)

    @property
    def session(self):
        """Get/create :class:`.RequestExtractionSession` for this context."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'extract'):
                ctx.retrieve = self.get_session()
            return ctx.retrieve
        return self.get_session()     # No application context.


requestExtraction = RequestExtraction()
