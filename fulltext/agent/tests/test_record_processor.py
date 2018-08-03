"""Unit tests for :mod:`search.agent`."""
import json
from unittest import TestCase, mock

from .. import consumer

# type: ignore


class TestIndexPaper(TestCase):
    """Re-index all versions of an arXiv paper."""

    def setUp(self):
        """Initialize a :class:`.MetadataRecordProcessor`."""
        self.checkpointer = mock.MagicMock()
        self.args = ('foo', '1', 'a1b2c3d4', 'qwertyuiop', 'us-east-1',
                     self.checkpointer)

    @mock.patch('boto3.client')
    @mock.patch(f'{consumer.__name__}.url_for')
    @mock.patch(f'{consumer.__name__}.extract_fulltext')
    def test_notify(self, mock_task, mock_url_for, mock_client_factory):
        """The arXiv paper has only one version."""
        mock_client = mock.MagicMock()
        mock_waiter = mock.MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client_factory.return_value = mock_client
        paper_id = '1602.00123v4'
        mock_url_for.return_value = f'http://foo/{paper_id}'
        data = json.dumps({'document_id': paper_id}).encode('utf-8')
        record = {'SequenceNumber': 'foo123', 'Data': data}
        processor = consumer.FulltextRecordProcessor(*self.args)
        processor.process_record(record)
        self.assertTrue(mock_task.delay.called_with(paper_id, 'http://foo'))
