"""Tests for :mod:`.services.extractor`."""

import os
import tempfile
from unittest import TestCase, mock

from docker.errors import ContainerError, APIError
from flask import Flask

from . import extractor


class TestExtract(TestCase):
    """We have a PDF from which to extract text."""

    def setUp(self):
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

    @mock.patch(f'{extractor.__name__}.DockerClient')
    def test_extract_successfully(self, mock_DockerClient):
        """Perform a successful extraction."""
        mock_client = mock.MagicMock()
        mock_DockerClient.return_value = mock_client

        with self.app.app_context():
            self.assertEqual(extractor.do_extraction(self.path), 'hello world')

    @mock.patch(f'{extractor.__name__}.DockerClient')
    def test_container_error(self, mock_DockerClient):
        """There is a container error while running the extractor."""
        mock_client = mock.MagicMock()

        def raise_containererror(*args, **kwargs):
            raise ContainerError('container', 'exit_status', 'command',
                                 'image', 'stderr')

        mock_client.containers.run.side_effect = raise_containererror
        mock_DockerClient.return_value = mock_client

        with self.app.app_context():
            with self.assertRaises(RuntimeError):
                extractor.do_extraction(self.path)

    @mock.patch(f'{extractor.__name__}.DockerClient')
    def test_api_error(self, mock_DockerClient):
        """There is a Docker API error while running the extractor."""
        mock_client = mock.MagicMock()

        def raise_apiererror(*args, **kwargs):
            raise APIError('foo', 'bar')

        mock_client.containers.run.side_effect = raise_apiererror
        mock_DockerClient.return_value = mock_client

        with self.app.app_context():
            with self.assertRaises(RuntimeError):
                extractor.do_extraction(self.path)

    @mock.patch(f'{extractor.__name__}.DockerClient')
    def test_no_output_file(self, mock_DockerClient):
        """The extractor does not generate an output file."""
        os.unlink(self.outpath)
        mock_client = mock.MagicMock()
        mock_DockerClient.return_value = mock_client

        with self.app.app_context():
            with self.assertRaises(extractor.NoContentError):
                extractor.do_extraction(self.path)

    @mock.patch(f'{extractor.__name__}.DockerClient')
    def test_output_file_is_empty(self, mock_DockerClient):
        """The extractor generates an empty output file."""
        with open(self.outpath, 'w') as f:
            f.write('')

        mock_client = mock.MagicMock()
        mock_DockerClient.return_value = mock_client

        with self.app.app_context():
            with self.assertRaises(extractor.NoContentError):
                extractor.do_extraction(self.path)