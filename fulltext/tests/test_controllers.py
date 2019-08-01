"""Tests for :mod:`fulltext.controllers`."""

from http import HTTPStatus as status
from unittest import TestCase, mock

from werkzeug.exceptions import InternalServerError

from .. import controllers


class TestStatusEndpoint(TestCase):
    """Tests for :func:`.controllers.service_status`."""

    @mock.patch(f'{controllers.__name__}.extract')
    @mock.patch(f'{controllers.__name__}.store.Storage')
    def test_all_available(self, mock_Storage, mock_extract):
        """All upstream services are available."""
        mock_storage = mock.MagicMock()
        mock_storage.is_available.return_value = True
        mock_Storage.current_session.return_value = mock_storage

        mock_extract.is_available.return_value = True

        data, code, headers = controllers.service_status()
        self.assertDictEqual(data, {'storage': True, 'extractor': True})
        self.assertEqual(code, status.OK, 'Returns 200 OK')

    @mock.patch(f'{controllers.__name__}.extract')
    @mock.patch(f'{controllers.__name__}.store.Storage')
    def test_storage_unavailable(self, mock_Storage, mock_extract):
        """Storage is unavailable."""
        mock_storage = mock.MagicMock()
        mock_storage.is_available.return_value = False
        mock_Storage.current_session.return_value = mock_storage

        mock_extract.is_available.return_value = True

        with self.assertRaises(InternalServerError):
            controllers.service_status()

    @mock.patch(f'{controllers.__name__}.extract')
    @mock.patch(f'{controllers.__name__}.store.Storage')
    def test_extractor_unavailable(self, mock_Storage, mock_extract):
        """Extractor is unavailable."""
        mock_storage = mock.MagicMock()
        mock_storage.is_available.return_value = True
        mock_Storage.current_session.return_value = mock_storage

        mock_extract.is_available.return_value = False

        with self.assertRaises(InternalServerError):
            controllers.service_status()
