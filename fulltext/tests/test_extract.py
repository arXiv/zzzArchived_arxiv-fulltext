"""Tests for :mod:`fulltext.extract`."""

import io
import os
import tempfile
from unittest import TestCase, mock

from flask import Flask

from arxiv.integration.api.exceptions import NotFound

from ..services import extractor
from ..services.extractor import Extractor
from ..services.legacy import CanonicalPDF
from ..services.store import Storage
from .. import extract


class TestExtract(TestCase):
    """We have a PDF from which to extract text."""

    def setUp(self) -> None:
        """Create an app."""
        self.workdir = tempfile.mkdtemp()
        self.app = Flask('foo')
        self.app.config.update({
            'WORKDIR': self.workdir,
            'MOUNTDIR': '/mountdir',
            'EXTRACTOR_IMAGE': 'arxiv/fooextractor',
            'EXTRACTOR_VERSION': '5.6.7',
            'DOCKER_HOST': 'tcp://foohost:2345'
        })
        _, self.path = tempfile.mkstemp(dir=self.workdir, suffix='.pdf')
        self.outpath = self.path.replace('.pdf', '.txt')
        with open(self.outpath, 'w') as f:
            f.write('hello world')
        with open(self.path.replace('.pdf', '.pdf2txt'), 'w') as f:
            f.write('hello pdf2txt')

    @mock.patch(f'{extract.__name__}.preview.PreviewService', mock.MagicMock())
    @mock.patch(f'{extract.__name__}.store.Storage')
    @mock.patch(f'{extract.__name__}.legacy.CanonicalPDF')
    def test_extract_no_canonical_pdf(self \
        , mock_CanonicalPDF: CanonicalPDF, mock_Storage: Storage) -> None:
        """Failure to retrieve a PDF."""
        # Mock store returns an extraction.
        mock_extraction = mock.MagicMock()
        mock_extraction.copy.return_value = mock_extraction
        mock_store = mock.MagicMock()
        mock_store.retrieve.return_value = mock_extraction
        mock_Storage.current_session.return_value = mock_store # type: ignore

        # Mock canonical raises a NotFound exception.
        def raise_not_found(*args: str, **kwargs: str) -> None:
            raise NotFound('foo', mock.MagicMock())

        mock_canonical = mock.MagicMock()
        mock_canonical.retrieve.side_effect = raise_not_found
        mock_CanonicalPDF.current_session.return_value = mock_canonical

        with self.app.app_context():
            with self.assertRaises(NotFound):   # Exception propagates.
                extract.extract('1234.56789', 'arxiv', '3.4.5')

        # Extraction object is stored even after exception.
        mock_store.store.assert_called_once_with(mock_extraction)

    @mock.patch(f'{extract.__name__}.extractor')
    @mock.patch(f'{extract.__name__}.preview.PreviewService', mock.MagicMock())
    @mock.patch(f'{extract.__name__}.store.Storage')
    @mock.patch(f'{extract.__name__}.legacy.CanonicalPDF')
    def test_extract_failed(self, mock_CanonicalPDF: CanonicalPDF \
        , mock_Storage: Storage, mock_extractor: Extractor) -> None:
        """Text extraction fails."""
        # Mock store returns an extraction.
        mock_extraction = mock.MagicMock()
        mock_extraction.copy.return_value = mock_extraction
        mock_store = mock.MagicMock()
        mock_store.retrieve.return_value = mock_extraction
        mock_Storage.current_session.return_value = mock_store  # type: ignore

        # Mock canonical returns a PDF.
        mock_canonical = mock.MagicMock()
        mock_canonical.retrieve.return_value = io.BytesIO(b'foopdfcontent')
        mock_CanonicalPDF.current_session.return_value = mock_canonical

        # Extractor generates no text content.
        mock_extractor.do_extraction.side_effect = extractor.NoContentError # type: ignore

        with self.app.app_context():
            # Exception propagates.
            with self.assertRaises(extractor.NoContentError):
                extract.extract('1234.56789', 'arxiv', '3.4.5')

        # Extraction object is stored even after exception.
        mock_store.store.assert_called_once_with(mock_extraction)

    @mock.patch(f'{extract.__name__}.extractor')
    @mock.patch(f'{extract.__name__}.preview.PreviewService', mock.MagicMock())
    @mock.patch(f'{extract.__name__}.store.Storage')
    @mock.patch(f'{extract.__name__}.legacy.CanonicalPDF')
    def test_extract_succeeds(self, mock_CanonicalPDF: CanonicalPDF \
        , mock_Storage: Storage, mock_extractor: Extractor) -> None:
        """Text is successfully extracted."""
        # Mock store returns an extraction.
        mock_extraction = mock.MagicMock()
        mock_extraction.copy.return_value = mock_extraction
        mock_store = mock.MagicMock()
        mock_store.retrieve.return_value = mock_extraction
        mock_Storage.current_session.return_value = mock_store # type: ignore

        # Mock canonical returns a PDF.
        mock_canonical = mock.MagicMock()
        mock_canonical.retrieve.return_value = io.BytesIO(b'foopdfcontent')
        mock_CanonicalPDF.current_session.return_value = mock_canonical

        # Extractor generates no text content.
        mock_extractor.do_extraction.return_value = 'FOO pDf cOntent'  # type: ignore

        with self.app.app_context():
            result = extract.extract('1234.56789', 'arxiv', '3.4.5')

        self.assertEqual(result, mock_extraction.to_dict.return_value,
                         'Returns a dict representation of the extraction')

        # PSV content is generated.
        mock_extraction.copy.called_with(content='foo pdf content')

        # PSV content is stored.
        mock_store.store.assert_called_with(mock_extraction, 'psv')



