"""Tests for :mod:`.store`."""

import tempfile
import shutil
import os
import stat
import json
from datetime import datetime
from pytz import UTC
from unittest import TestCase, mock
from .. import store
from ...domain import Extraction, SupportedFormats, SupportedBuckets


class TestInitialize(TestCase):
    """We are instantiating the :class:`.Storage` integration with a volume."""

    def setUp(self):
        """We have a volume."""
        self.volume = tempfile.mkdtemp()

    def test_init(self):
        """The storage integration is initialized with a volume."""
        storage = store.Storage(self.volume)
        self.assertTrue(storage.is_available(),
                        "The storage service is available")

    def test_init_with_subvolume(self):
        """The storage integration is initialized with a non-existant path."""
        subvolume = os.path.join(self.volume, 'foo', 'baz')
        self.assertFalse(os.path.exists(subvolume), "Nonexistant subvolume")

        storage = store.Storage(subvolume)
        self.assertTrue(storage.is_available(),
                        "The storage service is available")
        self.assertTrue(os.path.exists(subvolume), "The subvolume is created")

    def test_init_with_no_write_access_to_create_paths(self):
        """The storage integration is initialized with an unwritable path."""
        subvolume = os.path.join(self.volume, 'foo', 'baz')
        os.chmod(self.volume, stat.S_IREAD)  # Owner can read.

        with self.assertRaises(store.ConfigurationError):
            store.Storage(subvolume)

    def test_init_with_no_write_access_to_create_files(self):
        """The storage integration is initialized with an unwritable path."""
        subvolume = os.path.join(self.volume, 'foo', 'baz')
        os.makedirs(subvolume)
        os.chmod(subvolume, stat.S_IREAD)  # Owner can read.

        with self.assertRaises(store.ConfigurationError):
            store.Storage(subvolume)

    def tearDown(self):
        """Remove the volume."""
        try:
            shutil.rmtree(self.volume)
        except PermissionError:
            pass


class TestRetrieveNonexistantExtraction(TestCase):
    """We are attempting to retrieve an extraction that does not exist."""

    def setUp(self):
        """We have an instance of the :class:`.Storage` integration."""
        self.volume = tempfile.mkdtemp()
        self.storage = store.Storage(self.volume)

    def test_retrieve_newstyle(self):
        """We request a non-existant extraction."""
        with self.assertRaises(store.DoesNotExist):
            self.storage.retrieve('1901.00123')
        with self.assertRaises(store.DoesNotExist):
            self.storage.retrieve('1901.00123', '1')

    def test_retrieve_legacy(self):
        """We request a non-existant extraction."""
        with self.assertRaises(store.DoesNotExist):
            self.storage.retrieve('cs/0001982')
        with self.assertRaises(store.DoesNotExist):
            self.storage.retrieve('cs/0001982', '1')

    def test_retrieve_submission(self):
        """We request a non-existant extraction."""
        with self.assertRaises(store.DoesNotExist):
            self.storage.retrieve('1234567',
                                  bucket=SupportedBuckets.SUBMISSION)
        with self.assertRaises(store.DoesNotExist):
            self.storage.retrieve('1234567', '1',
                                  bucket=SupportedBuckets.SUBMISSION)

    def tearDown(self):
        """Remove the volume."""
        shutil.rmtree(self.volume)


class TestRetrieveExtractionInProgress(TestCase):
    """An extraction is in progress, and we attempt to retrieve it."""

    def setUp(self):
        """We have a :class:`.Storage` integration."""
        self.volume = tempfile.mkdtemp()
        self.storage = store.Storage(self.volume)

    def tearDown(self):
        """Remove the volume."""
        shutil.rmtree(self.volume)

    def create_meta(self, identifier: str, version: str, bucket: str,
                    **kwargs) -> None:
        """Create a metadata record."""
        meta = {
            'identifier': identifier,
            'version': version,
            'bucket': bucket,
            'started': datetime.now(UTC).isoformat(),
            'task_id': f"{bucket}::{identifier}::{version}",
            'status': 'in_progress'
        }
        meta.update(kwargs)
        meta_path = self.storage._meta_path(identifier, version, bucket)
        self.storage.make_paths(meta_path)
        with open(meta_path, 'w') as f:
            json.dump(meta, f)

    def test_retrieve_legacy(self):
        """Retrieve extraction metadata for a legacy e-print."""
        self.create_meta('cs/0001982', '1', SupportedBuckets.ARXIV)

        extraction = self.storage.retrieve('cs/0001982')
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.IN_PROGRESS)
        self.assertIsNone(extraction.content)

        extraction = self.storage.retrieve('cs/0001982', '1')
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.IN_PROGRESS)
        self.assertIsNone(extraction.content)

    def test_retrieve_newstyle(self):
        """Retrieve extraction metadata for a newstyle e-print."""
        self.create_meta('1901.00123', '1', SupportedBuckets.ARXIV)

        extraction = self.storage.retrieve('1901.00123')
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.IN_PROGRESS)
        self.assertIsNone(extraction.content)

        extraction = self.storage.retrieve('1901.00123', '1')
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.IN_PROGRESS)
        self.assertIsNone(extraction.content)

    def test_retrieve_submission(self):
        """Retrieve extraction metadata for a submission."""
        self.create_meta('1234567', '1', SupportedBuckets.SUBMISSION)

        extraction = self.storage.retrieve('1234567',
                                           bucket=SupportedBuckets.SUBMISSION)
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.IN_PROGRESS)
        self.assertIsNone(extraction.content)

        extraction = self.storage.retrieve('1234567', '1',
                                           bucket=SupportedBuckets.SUBMISSION)
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.IN_PROGRESS)
        self.assertIsNone(extraction.content)


class TestRetrieveExtraction(TestCase):
    """We attempt to retrieve an extraction that exists."""

    def setUp(self):
        """We have a :class:`.Storage` integration."""
        self.volume = tempfile.mkdtemp()
        self.storage = store.Storage(self.volume)

    def tearDown(self):
        """Remove the volume."""
        shutil.rmtree(self.volume)

    def create_meta(self, identifier: str, version: str, bucket: str,
                    **kwargs) -> None:
        """Create a metadata record."""
        meta = {
            'identifier': identifier,
            'version': version,
            'bucket': bucket,
            'started': datetime.now(UTC).isoformat(),
            'task_id': f"{bucket}::{identifier}::{version}",
            'status': 'succeeded'
        }
        meta.update(kwargs)
        meta_path = self.storage._meta_path(identifier, version, bucket)
        self.storage.make_paths(meta_path)
        with open(meta_path, 'w') as f:
            json.dump(meta, f)

    def create_content(self, identifier: str, version: str, format: str,
                       bucket: str, content: str) -> None:
        """Create a content resource."""
        content_path = self.storage._path(identifier, version, format, bucket)
        self.storage.make_paths(content_path)
        with open(content_path, 'wb') as f:
            f.write(content.encode('utf-8'))

    def test_retrieve_legacy(self):
        """Retrieve extraction for a legacy e-print."""
        self.create_meta('cs/0001982', '1', SupportedBuckets.ARXIV)
        self.create_content('cs/0001982', '1', SupportedFormats.PLAIN,
                            SupportedBuckets.ARXIV, 'foöcontent')

        extraction = self.storage.retrieve('cs/0001982')
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertEqual(extraction.content, 'foöcontent')

        extraction = self.storage.retrieve('cs/0001982', '1')
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertEqual(extraction.content, 'foöcontent')

        extraction = self.storage.retrieve('cs/0001982', '1', meta_only=True)
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertIsNone(extraction.content)

    def test_retrieve_newstyle(self):
        """Retrieve extraction for a newstyle e-print."""
        self.create_meta('1901.00123', '1', SupportedBuckets.ARXIV)
        self.create_content('1901.00123', '1', SupportedFormats.PLAIN,
                            SupportedBuckets.ARXIV, 'foöcontent')

        extraction = self.storage.retrieve('1901.00123')
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertEqual(extraction.content, 'foöcontent')

        extraction = self.storage.retrieve('1901.00123', '1')
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertEqual(extraction.content, 'foöcontent')

        extraction = self.storage.retrieve('1901.00123', '1', meta_only=True)
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertIsNone(extraction.content)

    def test_retrieve_submission(self):
        """Retrieve extraction for a submission."""
        self.create_meta('1234567', '1', SupportedBuckets.SUBMISSION)
        self.create_content('1234567', '1', SupportedFormats.PLAIN,
                            SupportedBuckets.SUBMISSION, 'foöcontent')

        extraction = self.storage.retrieve('1234567',
                                           bucket=SupportedBuckets.SUBMISSION)
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertEqual(extraction.content, 'foöcontent')

        extraction = self.storage.retrieve('1234567', '1',
                                           bucket=SupportedBuckets.SUBMISSION)
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertEqual(extraction.content, 'foöcontent')

        extraction = self.storage.retrieve('1234567', '1', meta_only=True,
                                           bucket=SupportedBuckets.SUBMISSION)
        self.assertIsInstance(extraction, Extraction)
        self.assertEqual(extraction.status, Extraction.Status.SUCCEEDED)
        self.assertIsNone(extraction.content)


class TestStoreExtraction(TestCase):
    """We attempt to store a new extraction."""

    def setUp(self):
        """We have a :class:`.Storage` integration."""
        self.volume = tempfile.mkdtemp()
        self.storage = store.Storage(self.volume)

    def tearDown(self):
        """Remove the volume."""
        shutil.rmtree(self.volume)

    def test_store_meta_only(self):
        """Store only metadata."""
        bucket = SupportedBuckets.ARXIV
        identifier = '1901.00123'
        version = '2'
        fmt = SupportedFormats.PSV
        self.storage.store(
            Extraction(
                identifier=identifier,
                version=version,
                bucket=bucket,
                started=datetime.now(UTC),
                task_id=f"{bucket}::{identifier}::{version}",
                status=Extraction.Status.IN_PROGRESS,
                content=None
            ),
            fmt
        )
        meta_path = self.storage._meta_path(identifier, version, bucket)
        content_path = self.storage._path(identifier, version, fmt, bucket)

        self.assertTrue(os.path.exists(meta_path), "Metadata record created")
        self.assertFalse(os.path.exists(content_path), "Content record is not")

        with self.assertRaises(store.DoesNotExist):
            self.storage.retrieve(identifier, '1')

        extraction = self.storage.retrieve(identifier, version)
        self.assertIsNone(extraction.content)

    def test_store(self):
        """Store only metadata."""
        bucket = SupportedBuckets.ARXIV
        identifier = '1901.00123'
        version = '2'
        fmt = SupportedFormats.PLAIN
        self.storage.store(
            Extraction(
                identifier=identifier,
                version=version,
                bucket=bucket,
                started=datetime.now(UTC),
                task_id=f"{bucket}::{identifier}::{version}",
                status=Extraction.Status.SUCCEEDED,
                content='föcontent'
            ),
            fmt
        )
        meta_path = self.storage._meta_path(identifier, version, bucket)
        content_path = self.storage._path(identifier, version, fmt, bucket)

        self.assertTrue(os.path.exists(meta_path), "Metadata record created")
        self.assertTrue(os.path.exists(content_path), "Content record is too")

        with self.assertRaises(store.DoesNotExist):
            self.storage.retrieve(identifier, '1')

        extraction = self.storage.retrieve(identifier, version)
        self.assertEqual(extraction.content, "föcontent")
