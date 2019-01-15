"""Tests for :mod:`fulltext.services.store`."""

from unittest import TestCase, mock
from datetime import datetime
import io
import shutil
import tempfile
import os
from pytz import UTC

from .. import store
from ...domain import ExtractionProduct


class TestStore(TestCase):
    """Test storing content with :func:`.store.store`."""

    def setUp(self):
        """Get a temporary storage volume."""
        self.storage_volume = tempfile.mkdtemp()

    def tearDown(self):
        """Remove the temporary storage volume."""
        shutil.rmtree(self.storage_volume)

    @mock.patch(f'{store.__name__}.get_application_config')
    def test_store_content(self, mock_get_config):
        """Store content for an extraction."""
        version = '1.3'
        bucket = 'fulltext'
        mock_get_config.return_value = {
            'STORAGE_VOLUME': self.storage_volume,
            'VERSION': version,
        }
        content = b'foocontent'
        paper_id = '1234.5678v9'
        store.store(paper_id, content, bucket=bucket)
        key = f'{paper_id[:4]}/{paper_id}/{version}/plain'
        content_path = os.path.join(self.storage_volume, bucket, key)
        self.assertTrue(os.path.exists(content_path))
        with open(content_path, 'rb') as f:
            self.assertEqual(content, f.read())

    @mock.patch(f'{store.__name__}.get_application_config')
    def test_retrieve_version(self, mock_get_config):
        """Retrieve content for a specific extraction version."""
        paper_id = '1234.5678v9'
        version = '1.3'
        bucket = 'fulltext'

        mock_get_config.return_value = {
            'STORAGE_VOLUME': self.storage_volume,
            'VERSION': version
        }

        keys = [
            f'{paper_id[:4]}/{paper_id}/0.1/plain',
            f'{paper_id[:4]}/{paper_id}/0.5/plain',
            f'{paper_id[:4]}/{paper_id}/1.3/plain',
            f'{paper_id[:4]}/{paper_id}/2.1/plain',
            f'{paper_id[:4]}/{paper_id}/classic/plain'
        ]
        for key in keys:
            content_path = os.path.join(self.storage_volume, bucket, key)
            parent, _ = os.path.split(content_path)
            if not os.path.exists(parent):
                os.makedirs(parent)
            with open(content_path, 'wb') as f:
                f.write(key.encode('utf-8'))

        key = f'{paper_id[:4]}/{paper_id}/{version}/plain'
        content_path = os.path.join(self.storage_volume, bucket, key)
        product = store.retrieve(paper_id, version, bucket=bucket)

        self.assertIsInstance(product, ExtractionProduct)
        self.assertEqual(product.paper_id, paper_id)
        self.assertEqual(
            product.content,
            f'{paper_id[:4]}/{paper_id}/1.3/plain'.encode('utf-8')
        )
        self.assertEqual(product.version, version)

    @mock.patch(f'{store.__name__}.get_application_config')
    def test_retrieve_latest(self, mock_get_config):
        """Retrieve content for the latest extraction."""
        paper_id = '1234.5678v9'
        bucket = 'fulltext'
        version = '1.3'
        mock_get_config.return_value = {
            'STORAGE_VOLUME': self.storage_volume,
            'VERSION': version
        }
        keys = [
            f'{paper_id[:4]}/{paper_id}/0.1/plain',
            f'{paper_id[:4]}/{paper_id}/0.5/plain',
            f'{paper_id[:4]}/{paper_id}/1.3/plain',
            f'{paper_id[:4]}/{paper_id}/2.1/plain',
            f'{paper_id[:4]}/{paper_id}/classic/plain'
        ]
        for key in keys:
            content_path = os.path.join(self.storage_volume, bucket, key)
            parent, _ = os.path.split(content_path)
            if not os.path.exists(parent):
                os.makedirs(parent)
            with open(content_path, 'wb') as f:
                f.write(key.encode('utf-8'))

        product = store.retrieve(paper_id, bucket=bucket)

        self.assertIsInstance(product, ExtractionProduct)
        self.assertEqual(product.paper_id, paper_id)
        self.assertEqual(
            product.content,
            f'{paper_id[:4]}/{paper_id}/2.1/plain'.encode('utf-8')
        )
        self.assertEqual(product.version, '2.1')

    @mock.patch(f'{store.__name__}.get_application_config')
    def test_retrieve_classic(self, mock_get_config):
        """Retrieve classic version when none else are available."""
        paper_id = '1234.5678v9'
        bucket = 'fulltext'
        version = '1.3'

        mock_get_config.return_value = {
            'STORAGE_VOLUME': self.storage_volume,
            'VERSION': version
        }
        keys = [f'{paper_id[:4]}/{paper_id}/classic/plain']
        for key in keys:
            content_path = os.path.join(self.storage_volume, bucket, key)
            parent, _ = os.path.split(content_path)
            if not os.path.exists(parent):
                os.makedirs(parent)
            with open(content_path, 'wb') as f:
                f.write(key.encode('utf-8'))

        product = store.retrieve(paper_id, bucket=bucket)
        self.assertIsInstance(product, ExtractionProduct)
        self.assertEqual(product.paper_id, paper_id)
        self.assertEqual(
            product.content,
            f'{paper_id[:4]}/{paper_id}/classic/plain'.encode('utf-8')
        )
        self.assertEqual(product.version, 'classic')
        self.assertEqual(product.version, 'classic')

    @mock.patch(f'{store.__name__}.get_application_config')
    def test_no_extractions(self, mock_get_config):
        """No extractions exist for the paper."""
        paper_id = '1234.5678v9'
        version = '1.3'
        bucket = 'fulltext'
        mock_get_config.return_value = {
            'STORAGE_VOLUME': self.storage_volume,
            'VERSION': version
        }

        with self.assertRaises(store.DoesNotExist):
            store.retrieve(paper_id, bucket=bucket)
