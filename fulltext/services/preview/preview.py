"""Integration with the submission preview service."""

import io
from typing import Tuple, List, Any, Union, Optional, IO, Callable, Iterator, \
    IO
from http import HTTPStatus as status
from datetime import datetime

from typing_extensions import Literal
from mypy_extensions import TypedDict
from backports.datetime_fromisoformat import MonkeyPatch

from arxiv.base import logging
from arxiv.integration.api import service, exceptions

from ..util import ReadWrapper

MonkeyPatch.patch_fromisoformat()
logger = logging.getLogger(__name__)


class AlreadyExists(exceptions.BadRequest):
    """An attempt was made to deposit a preview that already exists."""


class PreviewMeta(TypedDict):
    added: str
    size_bytes: int
    checksum: str


class PreviewService(service.HTTPIntegration):
    """Represents an interface to the submission preview."""

    SERVICE = 'preview'
    VERSION = '0.0'

    class Meta:
        """Configuration for :class:`PreviewService` integration."""

        service_name = 'preview'

    def is_available(self, **kwargs: Any) -> bool:
        """Check our connection to the filesystem service."""
        timeout: float = kwargs.get('timeout', 0.2)
        try:
            response = self.request('head', '/status', timeout=timeout)
        except Exception as e:
            logger.error('Encountered error calling filesystem: %s', e)
            return False
        return bool(response.status_code == status.OK)

    def get_owner(self, identifier: str, token: str) -> Optional[str]:
        """Get the owner of a compilation product."""
        response = self.request('head', f'/{identifier}', token, stream=True)
        owner: Optional[str] = response.headers.get('ARXIV-OWNER', None)
        return owner

    def get(self, identifier: str, token: str) -> Tuple[IO[bytes], str]:
        """
        Retrieve the content of the PDF preview for a submission.

        Parameters
        ----------
        identifier : str
            Combination of the source ID and checksum:
            ``{source_id}/{checksum}``, where ``source_id`` is the unique
            identifier of the source package from which the preview was
            generated, and ``checksum`` is the URL-safe base64-encoded MD5 hash
            of the source package content.
        token : str
            Authnz token for the request.

        Returns
        -------
        :class:`io.BytesIO`
            Streaming content of the preview.
        str
            URL-safe base64-encoded MD5 hash of the preview content.

        """
        response = self.request('get', f'/{identifier}/content', token)
        preview_checksum = str(response.headers['ETag'])
        return ReadWrapper(response.iter_content), preview_checksum

    def does_exist(self, identifier: str, token: str) \
            -> Tuple[bool, Optional[str]]:
        """
        Determine whether or not a preview exists for an identifier.

        Parameters
        ----------
        identifier : str
            Combination of the source ID and checksum:
            ``{source_id}/{checksum}``, where ``source_id`` is the unique
            identifier of the source package from which the preview was
            generated, and ``checksum`` is the URL-safe base64-encoded MD5 hash
            of the source package content.
        token : str
            Authnz token for the request.

        Returns
        -------
        bool
        str
            URL-safe base64-encoded MD5 hash of the preview content.

        """
        response = self.request('head', f'/{identifier}/content', token)
        if response.status_code == status.OK:
            return True, str(response.headers['ETag'])
        return False, None
