"""Filesystem-based storage for plain text extraction."""

from typing import Optional, Any
import os
import shutil
import json
from datetime import datetime
from pytz import UTC
from backports.datetime_fromisoformat import MonkeyPatch

from flask import Flask

from arxiv.identifier import OLD_STYLE, STANDARD
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
        self._volume = volume

        if not os.path.exists(self._volume):
            try:
                os.makedirs(self._volume)
            except Exception as e:
                raise ConfigurationError("Cannot create storage volume") from e

        if not self.is_available():
            raise ConfigurationError("Cannot access storage volume")

    def is_available(self, **kwargs: Any) -> bool:
        """Determine whether storage is available."""
        test_name = f'test-{datetime.timestamp(datetime.now(UTC))}'
        test_paper_path = self._paper_path('test', test_name)
        test_path = os.path.join(test_paper_path, test_name)
        try:
            self._store(test_path, 'test_name')
        except StorageFailed as e:
            logger.error('Could not write: %s', e)
            return False
        shutil.rmtree(test_paper_path)
        return True

    def _paper_path(self, identifier: str, bucket: str) -> str:
        """
        Generate a base path for extraction from a particular resource.

        This should generate paths like:

        - Old-style e-print: /{volume}/arxiv/alg-geom/9204/9204001
        - New-style e-print: /{volume}/arxiv/1801/00123
        - Anything else: /{volume}/{bucket}/{identifier}

        """
        if OLD_STYLE.match(identifier):
            pre, num = identifier.split('/', 1)
            return os.path.join(self._volume, bucket, pre, num[:4], num)
        elif STANDARD.match(identifier):
            prefix = identifier.split('.', 1)[0]
            return os.path.join(self._volume, bucket, prefix, identifier)
        return os.path.join(self._volume, bucket, identifier)

    def _path(self, identifier: str, version: str, content_fmt: str,
              bucket: str) -> str:
        return os.path.join(self._paper_path(identifier, bucket),
                            version, content_fmt)

    def _meta_path(self, identifier: str, version: str, bucket: str) -> str:
        return os.path.join(self._paper_path(identifier, bucket),
                            version, 'meta.json')

    def _creation_time(self, path: str) -> datetime:
        return datetime.fromtimestamp(os.path.getmtime(path), tz=UTC)

    # TODO: consider refactoring this to support full semantic versions, rather
    # than only major.minor that can be expressed as a float. This package
    # has decent support: https://pypi.org/project/semver/
    def _latest_version(self, identifier: str,
                        bucket: str = SupportedBuckets.ARXIV) -> str:
        def _try_float(value: str) -> float:
            try:
                return float(value)
            except ValueError:
                logger.debug(f"non-float version {value}")
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

    def store(self, extraction: Extraction,
              content_fmt: Optional[str] = None) -> None:
        """Store an :class:`.Extraction`."""
        logger.debug('Store content format %s: %s', content_fmt,
                     extraction.content is not None)
        if content_fmt is not None and extraction.content is not None:
            logger.debug('Store content for %s', extraction.identifier)
            content_path = self._path(extraction.identifier,
                                      extraction.version,
                                      content_fmt, extraction.bucket)
            self._store(content_path, extraction.content)

        # Store metadata separately.
        meta = extraction.to_dict()
        meta.pop('content')
        meta_path = self._meta_path(extraction.identifier, extraction.version,
                                    extraction.bucket)
        logger.debug('Store metadata for %s at %s',
                     extraction.identifier, meta_path)
        self._store(meta_path, json.dumps(meta))

    def _store(self, path: str, content: str) -> None:
        logger.debug('store %i bytes at %s', len(content), path)
        try:    # Write metadata record.
            self.make_paths(path)
            with open(path, 'w') as f:
                f.write(content)
        except (IOError, PermissionError) as e:
            logger.error('Encountered error when writing: %s', e)
            raise StorageFailed("Could not store content") from e

    def retrieve(self, identifier: str, version: Optional[str] = None,
                 content_fmt: str = SupportedFormats.PLAIN,
                 bucket: str = SupportedBuckets.ARXIV,
                 meta_only: bool = False) -> Extraction:
        """Retrieve an :class:`.Extraction`."""
        content: Optional[str] = None
        logger.debug('Retrieve %s (v%s) for %s from %s', content_fmt,
                     version, identifier, bucket)
        if version is None:
            version = self._latest_version(identifier, bucket)
            logger.debug('Using latest version: %s', version)

        content_path = self._path(identifier, version, content_fmt, bucket)
        # TODO: for classic extractions (i.e. not generated through this app),
        # we will need to handle the case that there is no metadata available.
        # We should generate fallback metadata that can be used to instantiate
        # ``Extraction``, below.
        try:
            with open(self._meta_path(identifier, version, bucket)) as meta_fp:
                meta = json.load(meta_fp)

            # mypy does not know about fromisoformat yet, apparently.
            if 'started' in meta and meta['started']:
                meta['started'] = datetime.fromisoformat(meta['started'])   # type: ignore
            if 'ended' in meta and meta['ended']:
                meta['ended'] = datetime.fromisoformat(meta['ended'])   # type: ignore
            meta['status'] = Extraction.Status(meta['status'])
        except FileNotFoundError as e:
            logger.debug('File does not exist: %s', content_path)
            raise DoesNotExist("No such resource") from e

        # Get the extraction content.
        if not meta_only:
            try:
                with open(content_path, 'rb') as content_fp:
                    content = content_fp.read().decode('utf-8')

            except FileNotFoundError:
                # If the content is not here, it is likely because the
                # extraction is still in progress.
                logger.info('No %s content found for %s (extractor version '
                            '%s) in bucket %s', content_fmt, identifier,
                            version, bucket)
        assert meta['bucket'] == bucket
        logger.debug('Finished loading extraction')
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
        instance: 'Storage' = g.store
        return instance
