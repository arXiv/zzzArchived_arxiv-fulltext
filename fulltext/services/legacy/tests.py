"""Tests for :mod:`fulltext.services.pdf`."""

from unittest import TestCase, mock
import os

from flask import Flask

from arxiv import status
from arxiv.integration.api import service

from . import legacy


class TestExists(TestCase):
    """Tests for :func:`fulltext.services.legacy.CanonicalPDF.exists`."""

    def setUp(self):
        """Start an app so that we have some context."""
        self.app = Flask('test')
        legacy.CanonicalPDF.init_app(self.app)

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_pdf_exists(self, session):
        """A PDF exists at the passed URL."""
        mock_response = mock.MagicMock(status_code=status.HTTP_200_OK)
        session.return_value = mock.MagicMock(
            head=mock.MagicMock(return_value=mock_response)
        )
        with self.app.app_context():
            canonical = legacy.CanonicalPDF.current_session()
            self.assertTrue(canonical.exists('1234.56789'))

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_pdf_does_not_exist(self, session):
        """A PDF does not exist at the passed URL."""
        mock_response = mock.MagicMock(status_code=status.HTTP_404_NOT_FOUND)
        session.return_value = mock.MagicMock(
            head=mock.MagicMock(return_value=mock_response)
        )
        with self.app.app_context():
            canonical = legacy.CanonicalPDF.current_session()
            self.assertFalse(canonical.exists('1234.56789'))

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_pdf_returns_error(self, session):
        """An unexpected status was returned."""
        mock_response = mock.MagicMock(status_code=status.HTTP_403_FORBIDDEN)
        session.return_value = mock.MagicMock(
            head=mock.MagicMock(return_value=mock_response)
        )
        with self.app.app_context():
            with self.assertRaises(IOError):
                legacy.CanonicalPDF.current_session().exists('1234.56789')


class TestRetrieve(TestCase):
    """Tests for :func:`fulltext.services.legacy.CanonicalPDF.retrieve`."""

    def setUp(self):
        """Start an app so that we have some context."""
        self.app = Flask('test')
        legacy.CanonicalPDF.init_app(self.app)

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_pdf_exists(self, session):
        """A PDF exists at the passed URL."""
        mock_response = mock.MagicMock(
            status_code=status.HTTP_200_OK,
            iter_content=lambda size: iter([b'foo']),
            headers={'Content-Type': 'application/pdf'})
        session.return_value = mock.MagicMock(
            get=mock.MagicMock(return_value=mock_response)
        )
        document_id = '1234.56789'
        with self.app.app_context():
            l = legacy.CanonicalPDF.current_session()
            pdf_content = l.retrieve(document_id)

        self.assertEqual(pdf_content.read(), b'foo')

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_pdf_not_ready(self, session):
        """A PDF still needs to be rendered."""
        mock_html_response = mock.MagicMock(
            status_code=status.HTTP_200_OK,
            content=b'<html>foo</html>',
            headers={'Content-Type': 'text/html'}
        )
        mock_pdf_response = mock.MagicMock(
            status_code=status.HTTP_200_OK,
            iter_content=lambda size: iter([b'foo']),
            headers={'Content-Type': 'application/pdf'}
        )
        mock_get = mock.MagicMock(
            side_effect=[mock_html_response, mock_pdf_response]
        )
        session.return_value = mock.MagicMock(get=mock_get)
        document_id = '1234.56789'

        with self.app.app_context():
            canonical = legacy.CanonicalPDF.current_session()
            pdf_content = canonical.retrieve(document_id, sleep=0)

        self.assertEqual(pdf_content.read(), b'foo')
        self.assertEqual(mock_get.call_count, 2)

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_pdf_never_ready(self, session):
        """A PDF cannot be rendered."""
        mock_html_response = mock.MagicMock(
            status_code=status.HTTP_200_OK,
            content=b'<html>foo</html>',
            headers={'Content-Type': 'text/html'}
        )
        mock_get = mock.MagicMock(return_value=mock_html_response)
        session.return_value = mock.MagicMock(get=mock_get)
        with self.app.app_context():
            canonical = legacy.CanonicalPDF.current_session()
            with self.assertRaises(IOError):
                canonical.retrieve('1234.56789', sleep=0)
        self.assertGreater(mock_get.call_count, 2)

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_pdf_does_not_exist(self, session):
        """A PDF does not exist at the passed URL."""
        mock_response = mock.MagicMock(status_code=status.HTTP_404_NOT_FOUND)
        session.return_value = mock.MagicMock(
            get=mock.MagicMock(return_value=mock_response)
        )
        with self.app.app_context():
            canonical = legacy.CanonicalPDF.current_session()
            with self.assertRaises(legacy.DoesNotExist):
                canonical.retrieve('1234.56789')

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_pdf_returns_error(self, session):
        """An unexpected status was returned."""
        mock_response = mock.MagicMock(status_code=status.HTTP_403_FORBIDDEN)
        session.return_value = mock.MagicMock(
            get=mock.MagicMock(return_value=mock_response)
        )
        with self.app.app_context():
            with self.assertRaises(IOError):
                canonical = legacy.CanonicalPDF.current_session()
                canonical.retrieve('1234.56789')
