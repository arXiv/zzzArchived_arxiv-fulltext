"""Unit tests for :mod:`search.agent`."""
import json
from typing import Any
from unittest import TestCase, mock

from .. import consumer

class TestIndexPaper(TestCase):
    """Re-index all versions of an arXiv paper."""

    def setUp(self) -> None:
        """Initialize a :class:`.MetadataRecordProcessor`."""
        self.checkpointer = mock.MagicMock()
        self.args = ('foo', '1', 'a1b2c3d4', 'qwertyuiop', 'us-east-1',
                     self.checkpointer)

    @mock.patch('boto3.client')
    @mock.patch(f'{consumer.__name__}.url_for')
    @mock.patch(f'{consumer.__name__}.extract')
    def test_notify(self, mock_task: Any, mock_url_for: str\
      , mock_client_factory: Any) -> None:
        """The arXiv paper has only one version."""
        mock_client = mock.MagicMock()
        mock_waiter = mock.MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        mock_client_factory.return_value = mock_client
        identifier = '1602.00123v4'
        mock_url_for.return_value = f'http://foo/{identifier}'  # type: ignore
        data = json.dumps({'document_id': identifier}).encode('utf-8')
        record = {'SequenceNumber': 'foo123', 'Data': data}
        processor = consumer.FulltextRecordProcessor(*self.args, config={})
        processor.process_record(record)
        self.assertTrue(mock_task.delay.called_with(identifier, 'http://foo'))
