"""App tests for plaintext extraction service."""

import os
import shutil
from unittest import TestCase, mock
import tempfile
from http import HTTPStatus as status

import docker

from arxiv.users.auth import scopes
from arxiv.users.helpers import generate_token
from arxiv.integration.api import service

# Don't use the worker; do it all in this process.
os.environ['CELERY_ALWAYS_EAGER'] = '1'

from fulltext.factory import create_web_app
from fulltext.services import pdf, store, compiler
from fulltext import extract

root, _ = os.path.split(os.path.dirname(__file__))
pdf_path = os.path.join(root, 'extractor', 'tests', 'pdfs', '1702.07336.pdf')


class TestApplication(TestCase):
    """
    End-to-end tests for the entire application.

    The differences between these tests and a production scenario are that:

    - We are running this locally, and not in containers.
    - We are mocking the PDF endpoints (e.g. compiler service).
    - We are running the asynchronous parts synchronously in this process
      (so there is no Redis task queue involved).
    - We are running the extractor image on the host Docker service, rather
      than in a DinD container.

    Otherwise, this does real extraction with the real extractor image.
    """

    def setUp(self):
        """Initialize the application with a temporary storage volume."""
        self.extractor_image = 'arxiv/fulltext-extractor'
        self.extractor_version = '0.3'
        self.jwt_secret = 'thesecret'
        os.environ['JWT_SECRET'] = self.jwt_secret

        self.volume = tempfile.mkdtemp()
        self.work_dir = tempfile.mkdtemp()
        self.app = create_web_app()
        self.app.config.update({
            'JWT_SECRET': self.jwt_secret,
            'STORAGE_VOLUME': self.volume,
            'EXTRACTOR_IMAGE': self.extractor_image,
            'EXTRACTOR_VERSION': self.extractor_version,
            'DOCKER_HOST': 'unix://var/run/docker.sock',
            'WORKDIR': self.work_dir,
            'MOUNTDIR': self.work_dir,
        })
        self.client = self.app.test_client()

    def tearDown(self):
        """Remove the storage volume."""
        shutil.rmtree(self.volume)

    def test_get_nonexistant_extraction(self):
        """Request for a non-existant extraction from an arXiv e-print."""
        token = generate_token('1234', 'foo@user.com', 'foouser',
                               scope=[scopes.READ_COMPILE,
                                      scopes.CREATE_COMPILE,
                                      scopes.READ_FULLTEXT,
                                      scopes.CREATE_FULLTEXT])
        with self.app.app_context():
            response = self.client.get('/arxiv/2102.00123',
                                       headers={'Authorization': token})
        self.assertEqual(response.status_code, status.NOT_FOUND,
                         "Returns 404 Not Found")

    @mock.patch(
        f'{extract.__name__}.docker.DockerClient.return_value.containers.run'
    )
    @mock.patch(f'{service.__name__}.requests.Session')
    def text_extraction_fails(self, session, mock_docker_run):
        """Extraction of an e-print fails."""
        # Mock the responses to HEAD and GET requests for the e-print PDF.
        mock_response = mock.MagicMock()
        mock_response.status_code = status.OK
        mock_response.headers = {'Content-Type': 'application/pdf'}
        with open(pdf_path, 'rb') as f:
            mock_response.content = f.read()    # Return a real PDF.
        session.return_value.head.return_value.status_code = status.OK
        session.return_value.get.return_value = mock_response

        token = generate_token('1234', 'foo@user.com', 'foouser',
                               scope=[scopes.READ_COMPILE,
                                      scopes.CREATE_COMPILE,
                                      scopes.READ_FULLTEXT,
                                      scopes.CREATE_FULLTEXT])
        mock_docker_run.side_effect = docker.errors.ContainerError

        # Since we are running Celery in "eager" mode for these tests, the
        # extraction will block and run here.
        with self.app.app_context():
            response = self.client.post('/arxiv/2102.00125',
                                        headers={'Authorization': token})

        self.assertEqual(response.status_code, status.ACCEPTED,
                         "Returns 202 Accepted")
        self.assertEqual(response.headers['Location'],
                         'http://localhost/arxiv/2102.00125/status',
                         "Redirects to task status endpoint")

        # Verify that we don't do the same thing twice.
        with self.app.app_context():
            response = self.client.post('/arxiv/2102.00125',
                                        headers={'Authorization': token})

        self.assertEqual(response.status_code, status.SEE_OTHER,
                         "Returns 303 See Other")
        self.assertEqual(response.headers['Location'],
                         'http://localhost/arxiv/2102.00125/status',
                         "Redirects to task status endpoint, since the task"
                         " has failed.")

        # The status endpoint will reflect the failure state.
        with self.app.app_context():
            response = self.client.get('/arxiv/2102.00125/status',
                                       headers={'Authorization': token})

        self.assertEqual(response.status_code, status.OK,
                         "Returns 200 OK")
        self.assertEqual(response.json['status'], 'failed', "Failed!")
        self.assertIsNone(response.json['content'], "No content is included")
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertIsNone(response.json['exception'], "No exception occurred")
        self.assertIsNone(response.json['owner'],
                          "This is an announced e-print; owner is not set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        # The extraction endpoint will reflect the failure state.
        with self.app.app_context():
            response = self.client.get('/arxiv/2102.00125',
                                       headers={'Authorization': token})

        self.assertEqual(response.status_code, status.OK,
                         "Returns 200 OK")
        self.assertEqual(response.json['status'], 'failed', "Failed!")
        self.assertIsNone(response.json['content'], "No content is included")
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertIsNone(response.json['exception'], "No exception occurred")
        self.assertIsNone(response.json['owner'],
                          "This is an announced e-print; owner is not set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        # We can re-start extraction by forcing.
        with self.app.app_context():
            response = self.client.post('/arxiv/2102.00125',
                                        json={'force': True},
                                        headers={'Authorization': token})

        self.assertEqual(response.status_code, status.ACCEPTED,
                         "Returns 202 Accepted")
        self.assertEqual(response.headers['Location'],
                         'http://localhost/arxiv/2102.00125/status',
                         "Redirects to task status endpoint")

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_request_extraction(self, session):
        """Request extraction of an (announced) arXiv e-print."""
        # Mock the responses to HEAD and GET requests for the e-print PDF.
        mock_response = mock.MagicMock()
        mock_response.status_code = status.OK
        mock_response.headers = {'Content-Type': 'application/pdf'}
        with open(pdf_path, 'rb') as f:
            mock_response.content = f.read()    # Return a real PDF.
        session.return_value.head.return_value.status_code = status.OK
        session.return_value.get.return_value = mock_response

        token = generate_token('1234', 'foo@user.com', 'foouser',
                               scope=[scopes.READ_COMPILE,
                                      scopes.CREATE_COMPILE,
                                      scopes.READ_FULLTEXT,
                                      scopes.CREATE_FULLTEXT])

        # Since we are running Celery in "eager" mode for these tests, the
        # extraction will block and run here.
        with self.app.app_context():
            response = self.client.post('/arxiv/2102.00123',
                                        headers={'Authorization': token})

        self.assertEqual(response.status_code, status.ACCEPTED,
                         "Returns 202 Accepted")
        self.assertEqual(response.headers['Location'],
                         'http://localhost/arxiv/2102.00123/status',
                         "Redirects to task status endpoint")

        # Verify that we don't do the same thing twice.
        with self.app.app_context():
            response = self.client.post('/arxiv/2102.00123',
                                        headers={'Authorization': token})

        self.assertEqual(response.status_code, status.SEE_OTHER,
                         "Returns 303 See Other")
        self.assertEqual(response.headers['Location'],
                         'http://localhost/arxiv/2102.00123',
                         "Redirects to task content endpoint, since the task"
                         " has already completed.")

        # Verify that authn/z requirements are enforced for extraction
        # endpoint.
        with self.app.app_context():
            unauthz = generate_token('1234', 'foo@user.com', 'foouser',
                                     scope=[scopes.READ_COMPILE,
                                            scopes.CREATE_COMPILE,
                                            scopes.READ_FULLTEXT])
            response = self.client.post('/arxiv/2102.00123',
                                        headers={'Authorization': unauthz})
            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:create scope is required")

            response = self.client.post('/arxiv/2102.00123')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication required to request extraction")

        # Since this is happening synchronously in these tests (see above),
        # we expect the task to have completed.
        with self.app.app_context():
            response = self.client.get('/arxiv/2102.00123/status',
                                       headers={'Authorization': token})

        self.assertEqual(response.status_code, status.SEE_OTHER,
                         "Returns 303 See Other")
        self.assertEqual(response.json['status'], 'succeeded', "Succeeded!")
        self.assertIsNone(response.json['content'], "No content is included")
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertIsNone(response.json['exception'], "No exception occurred")
        self.assertIsNone(response.json['owner'],
                          "This is an announced e-print; owner is not set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        self.assertEqual(response.headers['Location'],
                         'http://localhost/arxiv/2102.00123',
                         'Redirects to content')

        # Verify that authn/z requirements are enforced for status endpoint.
        with self.app.app_context():
            unauthz = generate_token('1234', 'foo@user.com', 'foouser',
                                     scope=[scopes.READ_COMPILE,
                                            scopes.CREATE_COMPILE,
                                            scopes.CREATE_FULLTEXT])
            response = self.client.get('/arxiv/2102.00123/status',
                                       headers={'Authorization': unauthz})
            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:read scope is required for status")

            response = self.client.get('/arxiv/2102.00123/status')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication is required to view status")

        # We should now be able to retrieve the content.
        with self.app.app_context():
            response = self.client.get('/arxiv/2102.00123',
                                       headers={'Authorization': token})

        self.assertEqual(response.status_code, status.OK, 'Returns 200 OK')
        self.assertEqual(response.json['status'], 'succeeded', "Succeeded!")
        self.assertIsNotNone(response.json['content'], 'Content is included')
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertIsNone(response.json['exception'], "No exception occurred")
        self.assertIsNone(response.json['owner'],
                          "This is an announced e-print; owner is not set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        # We should now be able to retrieve the PSV content, as well.
        with self.app.app_context():
            response = self.client.get('/arxiv/2102.00123/format/psv',
                                       headers={'Authorization': token})

        self.assertEqual(response.status_code, status.OK, 'Returns 200 OK')

        with self.app.app_context():
            # These should also work.
            valid_urls = [
                '/arxiv/2102.00123/version/0.3',
                '/arxiv/2102.00123/version/0.3/format/plain',
                '/arxiv/2102.00123/version/0.3/format/psv'
            ]
            for url in valid_urls:
                response = self.client.get(url,
                                           headers={'Authorization': token})
                self.assertEqual(response.status_code, status.OK,
                                 f'{url} should exist')

            # But not these.
            invalid_urls = [
                '/arxiv/2102.00123/version/0.2',
                '/arxiv/2102.00123/format/magic',
                '/arxiv/2102.00123/version/0.3/format/magic'
            ]
            for url in invalid_urls:
                response = self.client.get(url,
                                           headers={'Authorization': token})
                self.assertEqual(response.status_code, status.NOT_FOUND,
                                 f'{url} should not exist')

        # Verify that authn/z requirements are enforced for content endpoint.
        with self.app.app_context():
            unauthz = generate_token('1234', 'foo@user.com', 'foouser',
                                     scope=[scopes.READ_COMPILE,
                                            scopes.CREATE_COMPILE,
                                            scopes.CREATE_FULLTEXT])
            response = self.client.get('/arxiv/2102.00123',
                                       headers={'Authorization': unauthz})
            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:read scope is required for status")

            response = self.client.get('/arxiv/2102.00123')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication is required to view status")

    @mock.patch(f'{service.__name__}.requests.Session')
    def test_request_extraction_of_submission(self, session):
        """Request extraction of a submission."""
        user_id = '1234'
        # Mock the responses to HEAD and GET requests for the e-print PDF.
        mock_response = mock.MagicMock()
        mock_response.status_code = status.OK
        # The compiler service indicates the resource owner in the
        # `ARXIV-OWNER` header.
        mock_response.headers = {'Content-Type': 'application/pdf',
                                 'ARXIV-OWNER': user_id}
        with open(pdf_path, 'rb') as f:
            mock_response.content = f.read()    # Return a real PDF.
        session.return_value.head.return_value.status_code = status.OK
        session.return_value.head.return_value.headers \
            = {'ARXIV-OWNER': user_id}
        session.return_value.get.return_value = mock_response

        token = generate_token(user_id, 'foo@user.com', 'foouser',
                               scope=[scopes.READ_COMPILE,
                                      scopes.CREATE_COMPILE,
                                      scopes.READ_FULLTEXT,
                                      scopes.CREATE_FULLTEXT])

        # Since we are running Celery in "eager" mode for these tests, the
        # extraction will block and run here.
        with self.app.app_context():
            response = self.client.post('/submission/12345/asdf12345',
                                        headers={'Authorization': token})

        self.assertEqual(response.status_code, status.ACCEPTED,
                         "Returns 202 Accepted")
        self.assertEqual(response.headers['Location'],
                         'http://localhost/submission/12345/asdf12345/status',
                         "Redirects to task status endpoint")

        # Verify that we don't do the same thing twice.
        with self.app.app_context():
            response = self.client.post('/submission/12345/asdf12345',
                                        headers={'Authorization': token})

        self.assertEqual(response.status_code, status.SEE_OTHER,
                         "Returns 303 See Other")
        self.assertEqual(response.headers['Location'],
                         'http://localhost/submission/12345/asdf12345',
                         "Redirects to task content endpoint, since the task"
                         " has already completed.")

        # Verify that authn/z requirements are enforced for extraction
        # endpoint.
        with self.app.app_context():
            unauthz = generate_token(user_id, 'foo@user.com', 'foouser',
                                     scope=[scopes.READ_COMPILE,
                                            scopes.CREATE_COMPILE,
                                            scopes.READ_FULLTEXT])
            response = self.client.post('/submission/12345/asdf12345',
                                        headers={'Authorization': unauthz})
            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:create scope is required")

            response = self.client.post('/submission/12345/asdf12345')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication required to request extraction")

            other = generate_token('1235', 'foo@user.com', 'foouser',
                                   scope=[scopes.READ_COMPILE,
                                          scopes.CREATE_COMPILE,
                                          scopes.CREATE_FULLTEXT,
                                          scopes.READ_FULLTEXT])
            response = self.client.post('/submission/12345/asdf12345',
                                        headers={'Authorization': other})
            self.assertEqual(response.status_code, status.NOT_FOUND,
                             "Not the owner; pretend it does not exist")

        # Since this is happening synchronously in these tests (see above),
        # we expect the task to have completed.
        with self.app.app_context():
            response = self.client.get('/submission/12345/asdf12345/status',
                                       headers={'Authorization': token})

        self.assertEqual(response.status_code, status.SEE_OTHER,
                         "Returns 303 See Other")
        self.assertEqual(response.json['status'], 'succeeded', "Succeeded!")
        self.assertIsNone(response.json['content'], "No content is included")
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertIsNone(response.json['exception'], "No exception occurred")
        self.assertEqual(response.json['owner'], user_id,
                         "This is a submission; owner is set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        self.assertEqual(response.headers['Location'],
                         'http://localhost/submission/12345/asdf12345',
                         'Redirects to content')

        # Verify that authn/z requirements are enforced for status endpoint.
        with self.app.app_context():
            unauthz = generate_token('1234', 'foo@user.com', 'foouser',
                                     scope=[scopes.READ_COMPILE,
                                            scopes.CREATE_COMPILE,
                                            scopes.CREATE_FULLTEXT])
            response = self.client.get('/submission/12345/asdf12345/status',
                                       headers={'Authorization': unauthz})
            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:read scope is required for status")

            response = self.client.get('/submission/12345/asdf12345/status')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication is required to view status")

            other = generate_token('1235', 'foo@user.com', 'foouser',
                                   scope=[scopes.READ_COMPILE,
                                          scopes.CREATE_COMPILE,
                                          scopes.CREATE_FULLTEXT,
                                          scopes.READ_FULLTEXT])
            response = self.client.get('/submission/12345/asdf12345/status',
                                       headers={'Authorization': other})
            self.assertEqual(response.status_code, status.NOT_FOUND,
                             "Not the owner; pretend it does not exist")

        # We should now be able to retrieve the content,
        with self.app.app_context():
            response = self.client.get('/submission/12345/asdf12345',
                                       headers={'Authorization': token})

        self.assertEqual(response.status_code, status.OK, 'Returns 200 OK')
        self.assertEqual(response.json['status'], 'succeeded', "Succeeded!")
        self.assertIsNotNone(response.json['content'], 'Content is included')
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertIsNone(response.json['exception'], "No exception occurred")
        self.assertEqual(response.json['owner'], user_id,
                         "This is a submission; owner is set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        # We should now be able to retrieve the PSV content, as well.
        with self.app.app_context():
            response = self.client.get(
                '/submission/12345/asdf12345/format/psv',
                headers={'Authorization': token}
            )

        self.assertEqual(response.status_code, status.OK, 'Returns 200 OK')

        with self.app.app_context():
            # These should also work.
            valid_urls = [
                '/submission/12345/asdf12345/version/0.3',
                '/submission/12345/asdf12345/version/0.3/format/plain',
                '/submission/12345/asdf12345/version/0.3/format/psv'
            ]
            for url in valid_urls:
                response = self.client.get(url,
                                           headers={'Authorization': token})
                self.assertEqual(response.status_code, status.OK,
                                 f'{url} should exist')

            # But not these.
            invalid_urls = [
                '/submission/12345/asdf12345/version/0.2',
                '/submission/12345/asdf12345/format/magic',
                '/submission/12345/asdf12345/version/0.3/format/magic'
            ]
            for url in invalid_urls:
                response = self.client.get(url,
                                           headers={'Authorization': token})
                self.assertEqual(response.status_code, status.NOT_FOUND,
                                 f'{url} should not exist')

        # Verify that authn/z requirements are enforced for content endpoint.
        with self.app.app_context():
            unauthz = generate_token('1234', 'foo@user.com', 'foouser',
                                     scope=[scopes.READ_COMPILE,
                                            scopes.CREATE_COMPILE,
                                            scopes.CREATE_FULLTEXT])
            response = self.client.get('/submission/12345/asdf12345',
                                       headers={'Authorization': unauthz})
            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:read scope is required for status")

            response = self.client.get('/submission/12345/asdf12345')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication is required to view status")

            other = generate_token('1235', 'foo@user.com', 'foouser',
                                   scope=[scopes.READ_COMPILE,
                                          scopes.CREATE_COMPILE,
                                          scopes.CREATE_FULLTEXT,
                                          scopes.READ_FULLTEXT])
            response = self.client.get('/submission/12345/asdf12345',
                                       headers={'Authorization': other})
            self.assertEqual(response.status_code, status.NOT_FOUND,
                             "Not the owner; pretend it does not exist")
