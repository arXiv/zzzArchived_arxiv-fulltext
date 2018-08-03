"""Tests for :mod:`fulltext.services.store`."""

from unittest import TestCase, mock

from .. import store


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
