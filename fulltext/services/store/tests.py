"""Tests for :mod:`fulltext.services.store`."""

from unittest import TestCase, mock
from datetime import datetime
import io
from pytz import UTC

from .. import store
from ...domain import ExtractionProduct


class TestStore(TestCase):
    """Test storing content with :func:`.store.store`."""

    @mock.patch(f'{store.__name__}.get_application_config')
    @mock.patch(f'{store.__name__}.boto3.client')
    def test_store_content(self, mock_client_factory, mock_get_config):
        """Store content for an extraction in S3."""
        mock_client = mock.MagicMock()
        mock_waiter = mock.MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client_factory.return_value = mock_client
        version = '1.3'
        bucket = 'arxiv-fulltext'
        mock_get_config.return_value = {
            'AWS_REGION': 'us-east-1',
            'S3_BUCKETS': [('arxiv', bucket)],
            'VERSION': version
        }
        content = 'foocontent'
        paper_id = '1234.5678v9'
        store.store(paper_id, content)
        self.assertTrue(mock_client.put_object.called_with(
            Key=f'{paper_id}/{version}/plain',
            Bucket=bucket,
            Body=content.encode('utf-8')
        ))

    @mock.patch(f'{store.__name__}.get_application_config')
    @mock.patch(f'{store.__name__}.boto3.client')
    def test_retrieve_version(self, mock_client_factory, mock_get_config):
        """Retrieve content for a specific extraction version in S3."""
        paper_id = '1234.5678v9'
        content = b'foocontent'
        mock_client = mock.MagicMock()
        mock_waiter = mock.MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client.list_objects.return_value = {'Contents': [
            {'key': f'{paper_id}/0.1/plain'},
            {'key': f'{paper_id}/0.5/plain'},
            {'key': f'{paper_id}/1.3/plain'},
            {'key': f'{paper_id}/2.1/plain'},
            {'key': f'{paper_id}/classic/plain'}
        ]}
        created = datetime.now(UTC)
        mock_client.get_object.return_value = {
            'Body': io.BytesIO(content),
            'ETag': '-footag-',
            'LastModified': created
        }
        mock_client_factory.return_value = mock_client
        version = '1.3'
        bucket = 'arxiv-fulltext'
        mock_get_config.return_value = {
            'AWS_REGION': 'us-east-1',
            'S3_BUCKETS': [('arxiv', bucket)],
            'VERSION': version
        }

        product = store.retrieve(paper_id, version)
        self.assertTrue(mock_client.get_object.called_with(
            Key=f'{paper_id}/{version}/plain',
            Bucket=bucket
        ))
        self.assertIsInstance(product, ExtractionProduct)
        self.assertEqual(product.paper_id, paper_id)
        self.assertEqual(product.content, content.decode('utf-8'))
        self.assertEqual(product.version, version)
        self.assertEqual(product.etag, 'footag'),
        self.assertEqual(product.created, created)

    @mock.patch(f'{store.__name__}.get_application_config')
    @mock.patch(f'{store.__name__}.boto3.client')
    def test_retrieve_latest(self, mock_client_factory, mock_get_config):
        """Retrieve content for the latest extraction in S3."""
        paper_id = '1234.5678v9'
        content = b'foocontent'
        mock_client = mock.MagicMock()
        mock_waiter = mock.MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client.list_objects.return_value = {'Contents': [
            {'key': f'{paper_id}/0.1/plain'},
            {'key': f'{paper_id}/0.5/plain'},
            {'key': f'{paper_id}/1.3/plain'},
            {'key': f'{paper_id}/2.1/plain'},
            {'key': f'{paper_id}/classic/plain'}
        ]}
        created = datetime.now(UTC)
        mock_client.get_object.return_value = {
            'Body': io.BytesIO(content),
            'ETag': '-footag-',
            'LastModified': created,
        }
        mock_client_factory.return_value = mock_client
        bucket = 'arxiv-fulltext'
        mock_get_config.return_value = {
            'AWS_REGION': 'us-east-1',
            'S3_BUCKETS': [('arxiv', bucket)],
            'VERSION': '1.3'
        }
        product = store.retrieve(paper_id)
        self.assertTrue(mock_client.get_object.called_with(
            Key=f'{paper_id}/2.1/plain',
            Bucket=bucket
        ))

        self.assertIsInstance(product, ExtractionProduct)
        self.assertEqual(product.paper_id, paper_id)
        self.assertEqual(product.content, content.decode('utf-8'))
        self.assertEqual(product.version, '2.1')
        self.assertEqual(product.etag, 'footag'),
        self.assertEqual(product.created, created)

    @mock.patch(f'{store.__name__}.get_application_config')
    @mock.patch(f'{store.__name__}.boto3.client')
    def test_retrieve_classic(self, mock_client_factory, mock_get_config):
        """Retrieve classic version when none else are available."""
        paper_id = '1234.5678v9'
        content = b'foocontent'
        mock_client = mock.MagicMock()
        mock_waiter = mock.MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client.list_objects.return_value = {'Contents': [
            {'key': f'{paper_id}/classic/plain'}
        ]}
        created = datetime.now(UTC)
        mock_client.get_object.return_value = {
            'Body': io.BytesIO(content),
            'ETag': '-footag-',
            'LastModified': created,
        }
        mock_client_factory.return_value = mock_client
        bucket = 'arxiv-fulltext'
        mock_get_config.return_value = {
            'AWS_REGION': 'us-east-1',
            'S3_BUCKETS': [('arxiv', bucket)],
            'VERSION': '1.3'
        }
        product = store.retrieve(paper_id)
        self.assertTrue(mock_client.get_object.called_with(
            Key=f'{paper_id}/classic/plain',
            Bucket=bucket
        ))

        self.assertIsInstance(product, ExtractionProduct)
        self.assertEqual(product.paper_id, paper_id)
        self.assertEqual(product.content, content.decode('utf-8'))
        self.assertEqual(product.version, 'classic')
        self.assertEqual(product.etag, 'footag'),
        self.assertEqual(product.created, created)

    @mock.patch(f'{store.__name__}.get_application_config')
    @mock.patch(f'{store.__name__}.boto3.client')
    def test_no_extractions(self, mock_client_factory, mock_get_config):
        """No extractions exist for the paper."""
        paper_id = '1234.5678v9'
        mock_client = mock.MagicMock()
        mock_waiter = mock.MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client.list_objects.return_value = {'Contents': []}
        bucket = 'arxiv-fulltext'
        mock_get_config.return_value = {
            'AWS_REGION': 'us-east-1',
            'S3_BUCKETS': [('arxiv', bucket)],
            'VERSION': '1.3'
        }

        with self.assertRaises(store.DoesNotExist):
            store.retrieve(paper_id)
