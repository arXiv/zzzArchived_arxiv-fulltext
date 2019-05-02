"""
Integration with the compiler service API.

The compiler is responsible for building PDF, DVI, and other goodies from
LaTeX sources. This is where we obtain the submission PDF from which to
extract text.
"""
from typing import Tuple, Mapping, Optional
import os
import tempfile
import json
import io
import re
from urllib.parse import urlparse

from arxiv.base import logging
from arxiv.integration.api import status, service, exceptions
from arxiv.integration.api.exceptions import *

logger = logging.getLogger(__name__)


class Compiler(service.HTTPIntegration):
    """Encapsulates a connection with the compiler service."""

    VERSION = "0.1"
    """Verison of the compiler service with which we are integrating."""

    NAME = "arxiv-compiler"
    """Name of the compiler service with which we are integrating."""

    class Meta:
        """Configuration for :class:`Classifier`."""

        service_name = "compiler"

    def get_service_status(self) -> dict:
        """Get the status of the compiler service."""
        return self.json('get', 'status')[0]

    def exists(self, identifier: str, token: str) -> bool:
        """Check whether a compilation product exists."""
        endpoint = f'/{identifier}/pdf/product'
        try:
            response = self.request('head', endpoint, token, stream=True)
            return response.status_code == status.OK
        except exceptions.NotFound:
            return False

    def owner(self, identifier: str, token: str) -> Optional[str]:
        """Get the owner of a compilation product."""
        endpoint = f'/{identifier}/pdf/product'
        response = self.request('head', endpoint, token, stream=True)
        return response.headers.get('ARXIV-OWNER', None)

    def retrieve(self, identifier: str, token: str) \
            -> Tuple[str, Optional[str]]:
        """
        Get the compilation product for an upload workspace, if it exists.

        Parameters
        ----------
        identifier : str
            Has the format `{source_id}/{checksum}`.
        token : str
            Auth token to obtain access to the submission PDF.

        Returns
        -------
        str
            Local filesystem path to the PDF.
        str or None
            User ID of the resource owner.

        """
        endpoint = f'/{identifier}/pdf/product'
        response = self.request('get', endpoint, token, stream=True)
        prefix = identifier.replace('/', '#')
        _, pdf_path = tempfile.mkstemp(prefix=prefix, suffix='.pdf')
        with open(pdf_path, 'wb') as f:
            f.write(response.content)
        os.chmod(pdf_path, 0o775)
        return pdf_path, response.headers.get('ARXIV-OWNER')
