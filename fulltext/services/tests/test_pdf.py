"""Tests for :mod:`fulltext.services.pdf`."""

from unittest import TestCase, mock
import os

from arxiv import status

from .. import pdf


class TestExists(TestCase):
    """Tests for :func:`fulltext.services.pdf.exists`."""

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_exists(self, Session):
        """A PDF exists at the passed URL."""
        mock_response = mock.MagicMock(status_code=status.HTTP_200_OK)
        Session.return_value = mock.MagicMock(
            head=mock.MagicMock(return_value=mock_response)
        )
        self.assertTrue(pdf.exists('https://arxiv.org/pdf/1234.56789'))

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_does_not_exist(self, Session):
        """A PDF does not exist at the passed URL."""
        mock_response = mock.MagicMock(status_code=status.HTTP_404_NOT_FOUND)
        Session.return_value = mock.MagicMock(
            head=mock.MagicMock(return_value=mock_response)
        )
        self.assertFalse(pdf.exists('https://arxiv.org/pdf/1234.56789'))

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_returns_error(self, Session):
        """An unexpected status was returned."""
        mock_response = mock.MagicMock(status_code=status.HTTP_403_FORBIDDEN)
        Session.return_value = mock.MagicMock(
            head=mock.MagicMock(return_value=mock_response)
        )
        with self.assertRaises(IOError):
            pdf.exists('https://arxiv.org/pdf/1234.56789')

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_is_not_in_arxiv(self, Session):
        """An URI outside the arXiv platform is requested."""
        mock_response = mock.MagicMock()
        Session.return_value = mock.MagicMock(
            head=mock.MagicMock(return_value=mock_response)
        )
        with self.assertRaises(ValueError):
            pdf.exists('https://foo.org/pdf/1234.56789')


class TestRetrieve(TestCase):
    """Tests for :func:`fulltext.services.pdf.retrieve`."""

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_exists(self, Session):
        """A PDF exists at the passed URL."""
        mock_response = mock.MagicMock(
            status_code=status.HTTP_200_OK,
            content=b'foo',
            headers={'Content-Type': 'application/pdf'})
        Session.return_value = mock.MagicMock(
            get=mock.MagicMock(return_value=mock_response)
        )
        document_id = '1234.56789'
        pdf_path = pdf.retrieve('https://arxiv.org/pdf/1234.56789',
                                document_id)
        self.assertIn(document_id, pdf_path)
        self.assertTrue(pdf_path.endswith('.pdf'))
        self.assertTrue(os.path.exists(pdf_path))

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_not_ready(self, Session):
        """A PDF still needs to be rendered."""
        mock_html_response = mock.MagicMock(
            status_code=status.HTTP_200_OK,
            content=b'<html>foo</html>',
            headers={'Content-Type': 'text/html'}
        )
        mock_pdf_response = mock.MagicMock(
            status_code=status.HTTP_200_OK,
            content=b'foo',
            headers={'Content-Type': 'application/pdf'}
        )
        mock_get = mock.MagicMock(
            side_effect=[mock_html_response, mock_pdf_response]
        )
        Session.return_value = mock.MagicMock(get=mock_get)
        document_id = '1234.56789'
        pdf_path = pdf.retrieve('https://arxiv.org/pdf/1234.56789',
                                document_id, sleep=0)
        self.assertIn(document_id, pdf_path)
        self.assertTrue(pdf_path.endswith('.pdf'))
        self.assertTrue(os.path.exists(pdf_path))
        self.assertEqual(mock_get.call_count, 2)

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_never_ready(self, Session):
        """A PDF cannot be rendered."""
        mock_html_response = mock.MagicMock(
            status_code=status.HTTP_200_OK,
            content=b'<html>foo</html>',
            headers={'Content-Type': 'text/html'}
        )
        mock_get = mock.MagicMock(return_value=mock_html_response)
        Session.return_value = mock.MagicMock(get=mock_get)
        document_id = '1234.56789'
        with self.assertRaises(IOError):
            pdf.retrieve('https://arxiv.org/pdf/1234.56789', document_id,
                         sleep=0)
        self.assertGreater(mock_get.call_count, 2)

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_does_not_exist(self, Session):
        """A PDF does not exist at the passed URL."""
        mock_response = mock.MagicMock(status_code=status.HTTP_404_NOT_FOUND)
        Session.return_value = mock.MagicMock(
            get=mock.MagicMock(return_value=mock_response)
        )
        with self.assertRaises(pdf.DoesNotExist):
            pdf.retrieve('https://arxiv.org/pdf/1234.56789', '1234.56789')

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_returns_error(self, Session):
        """An unexpected status was returned."""
        mock_response = mock.MagicMock(status_code=status.HTTP_403_FORBIDDEN)
        Session.return_value = mock.MagicMock(
            get=mock.MagicMock(return_value=mock_response)
        )
        with self.assertRaises(IOError):
            pdf.retrieve('https://arxiv.org/pdf/1234.56789', '1234.56789')

    @mock.patch(f'{pdf.__name__}.requests.Session')
    def test_pdf_is_not_in_arxiv(self, Session):
        """An URI outside the arXiv platform is requested."""
        mock_response = mock.MagicMock()
        Session.return_value = mock.MagicMock(
            get=mock.MagicMock(return_value=mock_response)
        )
        with self.assertRaises(ValueError):
            pdf.retrieve('https://foo.org/pdf/1234.56789', '1234.56789')
