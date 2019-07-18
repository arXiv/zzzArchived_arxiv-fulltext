"""
Integration with the compiler service API.

The compiler is responsible for building PDF, DVI, and other goodies from
LaTeX sources. This is where we obtain the submission PDF from which to
extract text.
"""
from typing import Tuple, Optional, Any
import os
import tempfile
from urllib.parse import urlparse

from arxiv.base import logging
from arxiv.integration.api import status, service, exceptions

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
        _stat: dict = self.json('get', 'status')[0]
        return _stat

    def is_available(self, **kwargs: Any) -> bool:
        """Determine whether the compiler service is available."""
        timeout: float = kwargs.get('timeout', 0.5)
        try:
            resp = self.request('get', '/status', timeout=timeout)
        except exceptions.RequestFailed as e:
            logger.error('Error calling compiler: %s', e)
            return False
        return bool(resp.status_code == status.OK)

    def exists(self, identifier: str, token: str) -> bool:
        """Check whether a compilation product exists."""
        endpoint = f'/{identifier}/pdf/product'
        try:
            response = self.request('head', endpoint, token, stream=True)
            return bool(response.status_code == status.OK)
        except exceptions.NotFound:
            return False

    def owner(self, identifier: str, token: str) -> Optional[str]:
        """Get the owner of a compilation product."""
        endpoint = f'/{identifier}/pdf/product'
        response = self.request('head', endpoint, token, stream=True)
        owner: Optional[str] = response.headers.get('ARXIV-OWNER', None)
        return owner

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
        os.chmod(pdf_path, 0o644)
        return pdf_path, response.headers.get('ARXIV-OWNER')
