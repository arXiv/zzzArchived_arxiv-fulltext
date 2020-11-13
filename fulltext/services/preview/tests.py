"""Integration tests for the preview service."""

import io
import os
import time
from http import HTTPStatus as status
from unittest import TestCase

import docker
from flask import Flask


from .preview import PreviewService, AlreadyExists, exceptions


class TestPreviewIntegration(TestCase):
    """Integration tests for the preview service module."""

    network: Flask
    localstack: Flask
    container: Flask
    app: Flask


    __test__  = bool(int(os.environ.get('WITH_INTEGRATION', '0')))

    @classmethod
    def setUpClass(cls) -> None:
        """Start up the preview service, backed by localstack S3."""
        client = docker.from_env()
        image = f'arxiv/{PreviewService.SERVICE}'
        client.images.pull(image, tag=PreviewService.VERSION)
        cls.network = client.networks.create('test-preview-network')
        cls.localstack = client.containers.run(
            'atlassianlabs/localstack',
            detach=True,
            ports={'4572/tcp': 5572},
            network='test-preview-network',
            name='localstack',
            environment={'USE_SSL': 'true'}
        )
        cls.container = client.containers.run(
            f'{image}:{PreviewService.VERSION}',
            detach=True,
            network='test-preview-network',
            ports={'8000/tcp': 8889},
            environment={'S3_ENDPOINT': 'https://localstack:4572',
                         'S3_VERIFY': '0',
                         'NAMESPACE': 'test'}
        )
        time.sleep(5)

        cls.app = Flask('test')
        cls.app.config.update({
            'PREVIEW_SERVICE_HOST': 'localhost',
            'PREVIEW_SERVICE_PORT': '8889',
            'PREVIEW_PORT_8889_PROTO': 'http',
            'PREVIEW_VERIFY': False,
            'PREVIEW_ENDPOINT': 'http://localhost:8889'

        })
        PreviewService.init_app(cls.app)

    @classmethod
    def tearDownClass(cls) -> None:
        """Tear down the preview service and localstack."""
        cls.container.kill()
        cls.container.remove()
        cls.localstack.kill()
        cls.localstack.remove()
        cls.network.remove()

    def test_get_status(self) -> None:
        """Get the status endpoint."""
        with self.app.app_context():
            pv = PreviewService.current_session()
            self.assertEqual(pv.get_status(), {'iam': 'ok'})

    def test_is_available(self) -> None:
        """Poll for availability."""
        with self.app.app_context():
            pv = PreviewService.current_session()
            self.assertTrue(pv.is_available())

    def test_retrieve(self) -> None:
        """Retrieve a preview."""
        with self.app.app_context():
            pv = PreviewService.current_session()
            content = io.BytesIO(b'foocontent')
            source_id = 1234
            checksum = 'foochex=='
            token = 'footoken'
            pv.request('put', f'/{source_id}/{checksum}/content',  token,
                       data=content, expected_code=[status.CREATED],
                       headers={'Content-type': 'application/pdf'})

            stream, preview_checksum = pv.get(f'{source_id}/{checksum}', token)
            self.assertEqual(stream.read(), b'foocontent')
            self.assertEqual(preview_checksum, 'ewrggAHdCT55M1uUfwKLEA==')

    def test_does_exist(self) -> None:
        """Check for the existance of a preview."""
        with self.app.app_context():
            pv = PreviewService.current_session()
            content = io.BytesIO(b'foocontent')
            source_id = 1235
            checksum = 'foochex=='
            token = 'footoken'
            pv.request('put', f'/{source_id}/{checksum}/content',  token,
                       data=content, expected_code=[status.CREATED],
                       headers={'Content-type': 'application/pdf'})

            ok, preview_checksum = pv.does_exist(f'{source_id}/{checksum}',
                                                 token)
            self.assertTrue(ok, 'Preview exists')
            self.assertEqual(preview_checksum, 'ewrggAHdCT55M1uUfwKLEA==')

    def test_get_nonexistant_preview(self) -> None:
        """Try to retrieve a non-existant preview."""
        with self.app.app_context():
            pv = PreviewService.current_session()
            with self.assertRaises(exceptions.NotFound):
                pv.get('9876/foochex==', 'footoken')

    def test_has_nonexistant_preview(self) -> None:
        """Try to retrieve a non-existant preview."""
        with self.app.app_context():
            pv = PreviewService.current_session()
            with self.assertRaises(exceptions.NotFound):
                pv.does_exist('9875/foochex==', 'footoken')