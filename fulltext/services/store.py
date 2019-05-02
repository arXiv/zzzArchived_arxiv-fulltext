"""Filesystem-based storage for plain text extraction."""

from typing import Tuple, Optional, Dict, Union, List, Any
import os
import json
from functools import wraps
from datetime import datetime
from pytz import UTC
from backports.datetime_fromisoformat import MonkeyPatch

from flask import Flask

from arxiv.integration.meta import MetaIntegration
from arxiv.base.globals import get_application_global, get_application_config
from arxiv.base import logging

from ..domain import Extraction, SupportedFormats, SupportedBuckets

logger = logging.getLogger(__name__)
MonkeyPatch.patch_fromisoformat()


class ConfigurationError(RuntimeError):
    """A config parameter is missing or invalid."""


class DoesNotExist(RuntimeError):
    """The requested fulltext content does not exist."""


class StorageFailed(RuntimeError):
    """Could not store content."""


class Storage(metaclass=MetaIntegration):
    """Provides storage integration."""

    def __init__(self, volume: str) -> None:
        """Check and set the storage volume."""
        if not os.path.exists(volume):
            try:
                os.makedirs(volume)
            except Exception as e:
                raise ConfigurationError("Cannot create storage volume") from e

        self._volume = volume

    def _paper_path(self, identifier: str, bucket: str) -> str:
        return os.path.join(self._volume, bucket, identifier[:4], identifier)

    def _path(self, identifier: str, version: str, content_format: str,
              bucket: str) -> str:
        return os.path.join(self._paper_path(identifier, bucket),
                            version, content_format)

    def _meta_path(self, identifier: str, version: str, bucket: str) -> str:
        return os.path.join(self._paper_path(identifier, bucket),
                            version, 'meta.json')

    def _creation_time(self, path: str) -> datetime:
        return datetime.fromtimestamp(os.path.getmtime(path), tz=UTC)

    def _latest_version(self, identifier: str,
                        bucket: str = SupportedBuckets.ARXIV) -> str:
        def _try_float(value: str) -> float:
            try:
                return float(value)
            except ValueError:
                return 0.0
        try:
            root_path = self._paper_path(identifier, bucket)
            paths = os.listdir(root_path)
        except FileNotFoundError as e:
            logger.debug('Extraction root path does not exist: %s', root_path)
            raise DoesNotExist("No extractions found") from e
        versions = sorted([p for p in paths if not p.startswith('.')],
                          key=_try_float)
        if not versions:
            logger.debug('Cannot find any versions for %s in %s',
                         identifier, bucket)
            raise DoesNotExist(f'No versions for {identifier} in {bucket}')
        return versions[-1]

    @staticmethod
    def make_paths(path: str) -> None:
        """Create any missing directories containing terminal ``path``."""
        parent, _ = os.path.split(path)
        if not os.path.exists(parent):
            logger.debug('Make paths to %s', parent)
            os.makedirs(parent)

    def ready(self) -> bool:
        """Check whether the storage volume is available."""
        # TODO: read/write check?
        return os.path.exists(self._volume)

    def store(self, extraction: Extraction,
              content_format: Optional[str] = None) -> None:
        """Store an :class:`.Extraction`."""
        logger.debug('Store content format %s: %s', content_format,
                     extraction.content is not None)
        if content_format is not None and extraction.content is not None:
            logger.debug('Store content for %s', extraction.identifier)
            content_path = self._path(extraction.identifier,
                                      extraction.version,
                                      content_format, extraction.bucket)
            logger.debug('Content path: %s', content_path)
            self.make_paths(content_path)
            logger.debug('Store %s content for %s',
                         content_format, extraction.identifier)
            try:
                with open(content_path, 'wb') as f:
                    f.write(extraction.content.encode('utf-8'))
            except IOError as e:
                raise StorageFailed("Could not store content") from e

        # Store metadata separately.
        meta = extraction.to_dict()
        meta.pop('content')
        meta_path = self._meta_path(extraction.identifier, extraction.version,
                                    extraction.bucket)
        logger.debug('Store metadata for %s at %s',
                     extraction.identifier, meta_path)
        self.make_paths(meta_path)
        try:    # Write metadata record.
            with open(meta_path, 'w') as f:
                json.dump(meta, f)
        except IOError as e:
            raise StorageFailed("Could not store content") from e

    def retrieve(self, identifier: str, version: Optional[str] = None,
                 content_format: str = SupportedFormats.PLAIN,
                 bucket: str = SupportedBuckets.ARXIV,
                 meta_only: bool = False) -> Extraction:
        """Retrieve an :class:`.Extraction`."""
        content: Optional[bytes] = None
        logger.debug('Retrieve %s (v%s) for %s from %s', content_format,
                     version, identifier, bucket)
        if version is None:
            version = self._latest_version(identifier, bucket)
        content_path = self._path(identifier, version, content_format, bucket)
        try:
            with open(self._meta_path(identifier, version, bucket)) as f:
                meta = json.load(f)
            if 'started' in meta and meta['started']:
                meta['started'] = datetime.fromisoformat(meta['started'])
            if 'ended' in meta and meta['ended']:
                meta['ended'] = datetime.fromisoformat(meta['ended'])
            meta['status'] = Extraction.Status(meta['status'])
        except FileNotFoundError as e:
            raise DoesNotExist("No such resource") from e

        # Get the extraction content.
        if not meta_only:
            try:
                with open(content_path, 'rb') as f:
                    content = f.read()

            except FileNotFoundError:
                # If the content is not here, it is likely because the
                # extraction is still in progress.
                logger.info('No %s content found for %s (extractor version '
                            '%s) in bucket %s', content_format, identifier,
                            version, bucket)
        assert meta['bucket'] == bucket
        return Extraction(content=content, **meta)

    @classmethod
    def init_app(cls, app: Flask) -> None:
        """Set defaults for required configuration parameters."""
        app.config.setdefault('STORAGE_VOLUME', '/tmp/storage')

    @classmethod
    def create_session(cls) -> 'Storage':
        """Create a new :class:`.Storage` instance."""
        config = get_application_config()
        volume = config.get('STORAGE_VOLUME', '/tmp/storage')
        return cls(volume)

    @classmethod
    def current_session(cls) -> 'Storage':
        """Get the current :class:`.Storage` instance for this application."""
        g = get_application_global()
        if g is None:
            return cls.create_session()
        if 'store' not in g:
            g.store = cls.create_session()
        return g.store
