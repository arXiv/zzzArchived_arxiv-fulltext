"""Filesystem-based storage for plain text extraction."""

from typing import Tuple, Optional, Dict, Union, List
import os
from functools import wraps
from datetime import datetime
from pytz import UTC

from flask import Flask

from arxiv.base.globals import get_application_global, get_application_config
from arxiv.base import logging

from ...domain import ExtractionProduct

logger = logging.getLogger(__name__)


class ConfigurationError(RuntimeError):
    """A config parameter is missing or invalid."""


class DoesNotExist(RuntimeError):
    """The requested fulltext content does not exist."""


class StorageFailed(RuntimeError):
    """Could not store content."""


class Storage(object):
    """Provides storage integration."""

    def __init__(self, volume: str, version: str) -> None:
        """Check and set the storage volume."""
        if not os.path.exists(volume):
            try:
                os.makedirs(volume)
            except Exception as e:
                raise ConfigurationError("Cannot create storage volume") from e

        self._volume = volume
        self._version = version
        logger.debug('Storage with volume %s, version: %s', volume, version)

    def _paper_path(self, paper_id: str, bucket: str) -> str:
        return os.path.join(self._volume, bucket, paper_id[:4], paper_id)

    def _path(self, paper_id: str, version: str, content_format: str,
              bucket: str) -> str:
        return os.path.join(self._paper_path(paper_id, bucket),
                            version, content_format)

    def _creation_time(self, path: str) -> datetime:
        return datetime.fromtimestamp(os.path.getmtime(path), tz=UTC)

    def _latest_version(self, paper_id: str, bucket: str = 'arxiv') -> str:
        def _try_float(value: str) -> float:
            try:
                return float(value)
            except ValueError:
                return 0.0
        try:
            paths = os.listdir(self._paper_path(paper_id, bucket))
        except FileNotFoundError as e:
            raise DoesNotExist("No extractions found") from e
        versions = sorted([p for p in paths if not p.startswith('.')],
                          key=_try_float)
        if not versions:
            raise DoesNotExist(f'No versions found for {paper_id} in {bucket}')
        return versions[-1]

    @staticmethod
    def make_paths(path: str) -> None:
        parent, _ = os.path.split(path)
        if not os.path.exists(parent):
            os.makedirs(parent)

    def ready(self) -> bool:
        """Check whether the storage volume is available."""
        # TODO: read/write check?
        return os.path.exists(self._volume)

    def store(self, paper_id: str, content: bytes,
              version: Optional[str] = None, content_format: str = 'plain',
              bucket: str = 'arxiv') -> None:
        if version is None:
            version = self._version

        content_path = self._path(paper_id, version, content_format, bucket)
        logger.debug('Store for paper %s, format %s, in bucket %s, at: %s',
                     paper_id, content_format, bucket, content_path)
        self.make_paths(content_path)
        try:
            with open(content_path, 'wb') as f:
                f.write(content)
        except IOError as e:
            raise StorageFailed("Could not store content") from e

    def retrieve(self, paper_id: str, version: Optional[str] = None,
                 content_format: str = 'plain', bucket: str = 'arxiv') \
            -> ExtractionProduct:
        if version is None:
            version = self._latest_version(paper_id, bucket)
        content_path = self._path(paper_id, version, content_format, bucket)
        try:
            with open(content_path, 'rb') as f:
                content = f.read()
        except FileNotFoundError as e:
            raise DoesNotExist("No such resource") from e
        return ExtractionProduct(**{
            'paper_id': paper_id,
            'content': content,
            'version': version,
            'format': content_format,
            'created': self._creation_time(content_path)
        })

    def exists(self, paper_id: str, version: Optional[str] = None,
               content_format: str = 'plain', bucket: str = 'arxiv') -> bool:
        content_path = self._path(paper_id, version, content_format, bucket)
        return os.path.exists(content_path)


@wraps(Storage.store)
def store(paper_id: str, content: bytes,
          version: Optional[str] = None, content_format: str = 'plain',
          bucket: str = 'arxiv') -> None:
    """Store fulltext content."""
    return current_instance().store(paper_id, content, version, content_format,
                                    bucket)


@wraps(Storage.retrieve)
def retrieve(paper_id: str, version: Optional[str] = None,
             content_format: str = 'plain', bucket: str = 'arxiv') \
        -> ExtractionProduct:
    """Retrieve fulltext content."""
    return current_instance().retrieve(
        paper_id, version, content_format, bucket
    )


@wraps(Storage.exists)
def exists(paper_id: str, version: Optional[str] = None,
           content_format: str = 'plain', bucket: str = 'arxiv') -> bool:
    """Check if fulltext content exists."""
    return current_instance().exists(paper_id, version, content_format, bucket)


@wraps(Storage.ready)
def ready() -> bool:
    """Determine whether the store is ready to handle requests."""
    return current_instance().ready()


def init_app(app: Flask) -> None:
    """Set defaults for required configuration parameters."""
    app.config.setdefault('STORAGE_VOLUME', '/tmp/storage')
    app.config.setdefault('VERSION', '0.0')


def create_instance() -> Storage:
    """Create a new :class:`.Storage` instance."""
    config = get_application_config()
    volume = config.get('STORAGE_VOLUME', '/tmp/storage')
    version = config.get('VERSION', '0.0')
    return Storage(volume, version)


def current_instance() -> Storage:
    """Get the current :class:`.Storage` instance for this application."""
    g = get_application_global()
    if g is None:
        return create_instance()
    if 'store' not in g:
        g.store = create_instance()
    return g.store
