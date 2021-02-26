"""App tests for plaintext extraction service."""

import os
import shutil
import tempfile
import time
from http import HTTPStatus as status
from threading import Thread
from unittest import TestCase, mock

import docker
from celery.bin import worker

# TODO: Temporarily disable session tests (due to arxiv-auth dep)
# (Also see many occurrences of generate_token/token commenting below)
#
# from arxiv.users.auth import scopes
# from arxiv.users.helpers import generate_token

from arxiv.integration.api import service

from fulltext.factory import create_web_app
from fulltext.services import store, preview
from fulltext import extract

root, _ = os.path.split(os.path.dirname(__file__))
pdf_path = os.path.join(root, 'extractor', 'tests', 'pdfs', '1702.07336.pdf')


class TestApplication(TestCase):
    """
    End-to-end tests for the entire application.

    The differences between these tests and a production scenario are that:

    - We are running this locally, and not in containers.
    - We are mocking the PDF endpoints (e.g. preview service).
    - We are running the extractor image on the host Docker service, rather
      than in a DinD container.

    Otherwise, this does real extraction with the real extractor image.
    """

    @classmethod
    def setUpClass(cls):
        """Start redis and a worker."""
        cls.extractor_image = 'arxiv/fulltext-extractor'
        cls.extractor_version = '0.3'
        cls.volume = tempfile.mkdtemp()
        cls.work_dir = tempfile.mkdtemp()
        cls.jwt_secret = 'thesecret'

        # Set this in the environ so that everyone sees the same thing.
        os.environ.update({
            'JWT_SECRET': cls.jwt_secret,
            'STORAGE_VOLUME': cls.volume,
            'EXTRACTOR_IMAGE': cls.extractor_image,
            'EXTRACTOR_VERSION': cls.extractor_version,
            'DOCKER_HOST': 'unix://var/run/docker.sock',
            'REDIS_ENDPOINT': 'localhost:6379',
            'BROKER_URL': 'redis://localhost:6379/0',
            'RESULT_BACKEND': 'redis://localhost:6379/0',
            'WORKDIR': cls.work_dir,
            'MOUNTDIR': cls.work_dir,
        })

        # Start redis for the task queue.
        cls.redis = docker.from_env().containers.run('redis', detach=True,
                                                     ports={'6379/tcp': 6379})

        cls.app = create_web_app()

        cls.MISSING_CASE = '1201.00111'
        cls.FAIL_CASE = '1205.00123'
        cls.SUCCESS_CASE = '1203.00123'
        cls.SUBMISSION_CASE = '12345/asdf12345'
        cls.user_id = '1234'

        def start_worker():
            # Mocks for PDF endpoints go here, because it is the worker that
            # making the requests.
            with mock.patch(f'{service.__name__}.requests.Session') as session:

                def head(url, *args, **kwargs):
                    if cls.MISSING_CASE in url:
                        return mock.MagicMock(status_code=status.NOT_FOUND)
                    mock_head_response = mock.MagicMock(
                        status_code=status.OK,
                        headers={}
                    )
                    if cls.SUBMISSION_CASE in url:
                        mock_head_response.headers['ARXIV-OWNER'] = cls.user_id
                        mock_head_response.headers['ETag'] = 'footag=='
                    return mock_head_response

                def get(url, *args, **kwargs):
                    mock_response = mock.MagicMock()
                    mock_response.status_code = status.OK
                    mock_response.headers = {'Content-Type': 'application/pdf'}
                    with open(pdf_path, 'rb') as f:
                        mock_response.iter_content.return_value \
                            = iter([f.read()])    # Return a real PDF.

                    if cls.MISSING_CASE in url:
                        return mock.MagicMock(status_code=status.NOT_FOUND)
                    elif cls.FAIL_CASE in url:
                        return mock.MagicMock(
                            status_code=status.INTERNAL_SERVER_ERROR
                        )
                    if cls.SUBMISSION_CASE in url:
                        mock_response.headers['ARXIV-OWNER'] = cls.user_id
                        mock_response.headers['ETag'] = 'footag=='
                    return mock_response
                session.return_value.head.side_effect = head    # return_value.status_code = status.OK
                session.return_value.get.side_effect = get      # return_value = mock_response

                with cls.app.app_context():
                    celery_app = extract.get_or_create_worker_app(cls.app)
                    worker.worker(app=celery_app).run()

        # Start the worker in a thread, so that it can run while we perform
        # our tests.
        time.sleep(2)
        t = Thread(target=start_worker)
        t.daemon = True
        t.start()
        time.sleep(2)

    @classmethod
    def tearDownClass(cls):
        """Stop redis."""
        cls.redis.kill()
        cls.redis.remove()

    def setUp(self):
        """Initialize the application with a temporary storage volume."""
        self.client = self.app.test_client()

    def tearDown(self):
        """Remove the storage volume."""
        shutil.rmtree(self.volume)

    def test_get_nonexistant_extraction(self):
        """Request for a non-existant extraction from an arXiv e-print."""
        # token = generate_token('1234', 'foo@user.com', 'foouser',
        #                        scope=[scopes.READ_COMPILE,
        #                               scopes.CREATE_COMPILE,
        #                               scopes.READ_FULLTEXT,
        #                               scopes.CREATE_FULLTEXT])
        with self.app.app_context():
            # response = self.client.get('/arxiv/2102.00123',
            #                            headers={'Authorization': token})
            response = self.client.get('/arxiv/2102.00123')

        self.assertEqual(response.status_code, status.NOT_FOUND,
                         "Returns 404 Not Found")

    def test_extraction_fails(self):
        """Extraction of an e-print fails."""
        # Mock the responses to HEAD and GET requests for the e-print PDF.
        # token = generate_token('1234', 'foo@user.com', 'foouser',
        #                        scope=[scopes.READ_COMPILE,
        #                               scopes.CREATE_COMPILE,
        #                               scopes.READ_FULLTEXT,
        #                               scopes.CREATE_FULLTEXT])

        with self.app.app_context():
            # response = self.client.post(f'/arxiv/{self.FAIL_CASE}',
            #                             headers={'Authorization': token})
            response = self.client.post(f'/arxiv/{self.FAIL_CASE}')

        self.assertEqual(response.status_code, status.ACCEPTED,
                         "Returns 202 Accepted")
        self.assertEqual(response.headers['Location'],
                         f'http://localhost/arxiv/{self.FAIL_CASE}/status',
                         "Redirects to task status endpoint")

        tries = 0
        # response = self.client.get(f'/arxiv/{self.FAIL_CASE}/status',
        #                            headers={'Authorization': token})
        response = self.client.get(f'/arxiv/{self.FAIL_CASE}/status')
        while True:
            if tries > 30:
                self.fail('Waited too long for result')
            time.sleep(2)
            with self.app.app_context():
                # response = self.client.get(f'/arxiv/{self.FAIL_CASE}/status',
                #                            headers={'Authorization': token})
                response = self.client.get(f'/arxiv/{self.FAIL_CASE}/status')

                response_data = response.json
                print('::', response_data)
                if response_data['status'] == 'failed':
                    break
                elif response_data['status'] == 'succeeded':
                    self.fail('Extraction should not succeed')
            tries += 1

        # The status endpoint will reflect the failure state.
        with self.app.app_context():
            # response = self.client.get(f'/arxiv/{self.FAIL_CASE}/status',
            #                            headers={'Authorization': token})
            response = self.client.get(f'/arxiv/{self.FAIL_CASE}/status')

        self.assertEqual(response.status_code, status.OK,
                         "Returns 200 OK")
        self.assertEqual(response.json['status'], 'failed', "Failed!")
        self.assertIsNone(response.json['content'], "No content is included")
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertEqual(response.json['exception'],
                         "1205.00123: unexpected status for PDF: 500")
        self.assertIsNone(response.json['owner'],
                          "This is an announced e-print; owner is not set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        # The extraction endpoint will reflect the failure state.
        with self.app.app_context():
            # response = self.client.get(f'/arxiv/{self.FAIL_CASE}',
            #                            headers={'Authorization': token})
            response = self.client.get(f'/arxiv/{self.FAIL_CASE}')

        self.assertEqual(response.status_code, status.OK,
                         "Returns 200 OK")
        self.assertEqual(response.json['status'], 'failed', "Failed!")
        self.assertIsNone(response.json['content'], "No content is included")
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertEqual(response.json['exception'],
                         "1205.00123: unexpected status for PDF: 500")
        self.assertIsNone(response.json['owner'],
                          "This is an announced e-print; owner is not set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        # We can re-start extraction by forcing.
        with self.app.app_context():
            response = self.client.post(f'/arxiv/{self.FAIL_CASE}',
                                        json={'force': True})
                                        #headers={'Authorization': token})

        self.assertEqual(response.status_code, status.ACCEPTED,
                         "Returns 202 Accepted")
        self.assertEqual(response.headers['Location'],
                         f'http://localhost/arxiv/{self.FAIL_CASE}/status',
                         "Redirects to task status endpoint")

    def test_request_extraction(self):
        """Request extraction of an (announced) arXiv e-print."""
        # Mock the responses to HEAD and GET requests for the e-print PDF.
        # token = generate_token('1234', 'foo@user.com', 'foouser',
        #                        scope=[scopes.READ_COMPILE,
        #                               scopes.CREATE_COMPILE,
        #                               scopes.READ_FULLTEXT,
        #                               scopes.CREATE_FULLTEXT])

        # Since we are running Celery in "eager" mode for these tests, the
        # extraction will block and run here.
        with self.app.app_context():
            # response = self.client.post(f'/arxiv/{self.SUCCESS_CASE}',
            #                             headers={'Authorization': token})
            response = self.client.post(f'/arxiv/{self.SUCCESS_CASE}')

        self.assertEqual(response.status_code, status.ACCEPTED,
                         "Returns 202 Accepted")
        self.assertEqual(response.headers['Location'],
                         f'http://localhost/arxiv/{self.SUCCESS_CASE}/status',
                         "Redirects to task status endpoint")

        # Verify that we don't do the same thing twice.
        with self.app.app_context():
            # response = self.client.post(f'/arxiv/{self.SUCCESS_CASE}',
            #                             headers={'Authorization': token})
            response = self.client.post(f'/arxiv/{self.SUCCESS_CASE}')

        self.assertEqual(response.status_code, status.SEE_OTHER,
                         "Returns 303 See Other")
        self.assertEqual(response.headers['Location'],
                         f'http://localhost/arxiv/{self.SUCCESS_CASE}/status',
                         "Redirects to task status endpoint, since the task"
                         " has not yet completed.")
        tries = 0
        response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}/status',
                                   headers={'Authorization': token})
        while response.status_code != status.SEE_OTHER:
            if tries > 30:
                self.fail('Waited too long for result')
            time.sleep(2)
            with self.app.app_context():
                response = self.client.get(
                    f'/arxiv/{self.SUCCESS_CASE}/status',
                    headers={'Authorization': token}
                )
            tries += 1

        response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}',
                                   headers={'Authorization': token})
        self.assertEqual(response.status_code, status.OK)

        # Verify that authn/z requirements are enforced for extraction
        # endpoint.
        with self.app.app_context():
            # unauthz = generate_token('1234', 'foo@user.com', 'foouser',
            #                          scope=[scopes.READ_COMPILE,
            #                                 scopes.CREATE_COMPILE,
            #                                 scopes.READ_FULLTEXT])
            # response = self.client.post(f'/arxiv/{self.SUCCESS_CASE}',
            #                             headers={'Authorization': unauthz})
            response = self.client.post(f'/arxiv/{self.SUCCESS_CASE}')

            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:create scope is required")

            response = self.client.post(f'/arxiv/{self.SUCCESS_CASE}')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication required to request extraction")

        # Since this is happening synchronously in these tests (see above),
        # we expect the task to have completed.
        with self.app.app_context():
            response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}/status',
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
                         f'http://localhost/arxiv/{self.SUCCESS_CASE}',
                         'Redirects to content')

        # Verify that authn/z requirements are enforced for status endpoint.
        with self.app.app_context():
            # unauthz = generate_token('1234', 'foo@user.com', 'foouser',
            #                          scope=[scopes.READ_COMPILE,
            #                                 scopes.CREATE_COMPILE,
            #                                 scopes.CREATE_FULLTEXT])
            # response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}/status',
            #                            headers={'Authorization': unauthz})
            response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}/status')
            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:read scope is required for status")

            response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}/status')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication is required to view status")

        # We should now be able to retrieve the content.
        with self.app.app_context():
            response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}',
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
            response = self.client.get(
                f'/arxiv/{self.SUCCESS_CASE}/format/psv',
                headers={'Authorization': token}
            )

        self.assertEqual(response.status_code, status.OK, 'Returns 200 OK')

        with self.app.app_context():
            # These should also work.
            valid_urls = [
                f'/arxiv/{self.SUCCESS_CASE}/version/0.3',
                f'/arxiv/{self.SUCCESS_CASE}/version/0.3/format/plain',
                f'/arxiv/{self.SUCCESS_CASE}/version/0.3/format/psv'
            ]
            for url in valid_urls:
                response = self.client.get(url,
                                           headers={'Authorization': token})
                self.assertEqual(response.status_code, status.OK,
                                 f'{url} should exist')

            # But not these.
            invalid_urls = [
                f'/arxiv/{self.SUCCESS_CASE}/version/0.2',
                f'/arxiv/{self.SUCCESS_CASE}/format/magic',
                f'/arxiv/{self.SUCCESS_CASE}/version/0.3/format/magic'
            ]
            for url in invalid_urls:
                response = self.client.get(url,
                                           headers={'Authorization': token})
                self.assertEqual(response.status_code, status.NOT_FOUND,
                                 f'{url} should not exist')

        # Verify that authn/z requirements are enforced for content endpoint.
        with self.app.app_context():
            # unauthz = generate_token('1234', 'foo@user.com', 'foouser',
            #                          scope=[scopes.READ_COMPILE,
            #                                 scopes.CREATE_COMPILE,
            #                                 scopes.CREATE_FULLTEXT])
            # response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}',
            #                            headers={'Authorization': unauthz})
            response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}')

            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:read scope is required for status")

            response = self.client.get(f'/arxiv/{self.SUCCESS_CASE}')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication is required to view status")

    def test_request_extraction_of_submission(self):
        """Request extraction of a submission."""
        # token = generate_token(self.user_id, 'foo@user.com', 'foouser',
        #                        scope=[scopes.READ_COMPILE,
        #                               scopes.CREATE_COMPILE,
        #                               scopes.READ_FULLTEXT,
        #                               scopes.CREATE_FULLTEXT])

        # Since we are running Celery in "eager" mode for these tests, the
        # extraction will block and run here.
        with self.app.app_context():
            # response = self.client.post(f'/submission/{self.SUBMISSION_CASE}',
            #                             headers={'Authorization': token})
            response = self.client.post(f'/submission/{self.SUBMISSION_CASE}')

        self.assertEqual(response.status_code, status.ACCEPTED,
                         "Returns 202 Accepted")
        self.assertEqual(response.headers['Location'],
                         f'http://localhost/submission/{self.SUBMISSION_CASE}/status',
                         "Redirects to task status endpoint")

        # Verify that authn/z requirements are enforced for extraction
        # endpoint.
        with self.app.app_context():
            # unauthz = generate_token(self.user_id, 'foo@user.com', 'foouser',
            #                          scope=[scopes.READ_COMPILE,
            #                                 scopes.CREATE_COMPILE,
            #                                 scopes.READ_FULLTEXT])
            # response = self.client.post(f'/submission/{self.SUBMISSION_CASE}',
            #                             headers={'Authorization': unauthz})
            response = self.client.post(f'/submission/{self.SUBMISSION_CASE}')

            # TODO: Temporarily disable session tests (due to arxiv-auth dep)
            # self.assertEqual(response.status_code, status.FORBIDDEN,
            #                  "The fulltext:create scope is required")

            response = self.client.post(f'/submission/{self.SUBMISSION_CASE}')
            # TODO: Temporarily disable session tests (due to arxiv-auth dep)
            # self.assertEqual(response.status_code, status.UNAUTHORIZED,
            #                  "Authentication required to request extraction")

            # other = generate_token('1235', 'foo@user.com', 'foouser',
            #                        scope=[scopes.READ_COMPILE,
            #                               scopes.CREATE_COMPILE,
            #                               scopes.CREATE_FULLTEXT,
            #                               scopes.READ_FULLTEXT])
            # response = self.client.post(f'/submission/{self.SUBMISSION_CASE}',
            #                             headers={'Authorization': other})
            response = self.client.post(f'/submission/{self.SUBMISSION_CASE}')

            # TODO: Temporarily disable session tests (due to arxiv-auth dep)
            # self.assertEqual(response.status_code, status.NOT_FOUND,
            #                  "Not the owner; pretend it does not exist")

        # Since this is happening assynchronously in these tests (see above),
        # we expect the task to have not completed.
        with self.app.app_context():
            # response = self.client.get(f'/submission/{self.SUBMISSION_CASE}/status',
            #                            headers={'Authorization': token})
            response = self.client.get(f'/submission/{self.SUBMISSION_CASE}/status')
        self.assertEqual(response.status_code, status.OK,
                         "Returns 200 OK")

        tries = 0
        while True:
            if tries > 30:
                self.fail('Waited too long')
            time.sleep(2)
            with self.app.app_context():
                response = self.client.get(
                    f'/submission/{self.SUBMISSION_CASE}/status'
                    # headers={'Authorization': token}
                )
                if response.json['status'] == 'failed':
                    self.fail('Extraction failed')
                elif response.json['status'] == 'succeeded':
                    break
            tries += 1

        self.assertEqual(response.json['status'], 'succeeded', "Succeeded!")
        self.assertIsNone(response.json['content'], "No content is included")
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertIsNone(response.json['exception'], "No exception occurred")
        self.assertEqual(response.json['owner'], self.user_id,
                         "This is a submission; owner is set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        self.assertEqual(response.headers['Location'],
                         f'http://localhost/submission/{self.SUBMISSION_CASE}',
                         'Redirects to content')

        # Verify that authn/z requirements are enforced for status endpoint.
        with self.app.app_context():
            # unauthz = generate_token('1234', 'foo@user.com', 'foouser',
            #                          scope=[scopes.READ_COMPILE,
            #                                 scopes.CREATE_COMPILE,
            #                                 scopes.CREATE_FULLTEXT])
            # response = self.client.get(f'/submission/{self.SUBMISSION_CASE}/status',
            #                            headers={'Authorization': unauthz})
            response = self.client.get(f'/submission/{self.SUBMISSION_CASE}/status')
            # TODO: Temporarily disable session tests (due to arxiv-auth dep)
            # self.assertEqual(response.status_code, status.FORBIDDEN,
            #                  "The fulltext:read scope is required for status")
            response = self.client.get(f'/submission/{self.SUBMISSION_CASE}/status')
            # TODO: Temporarily disable session tests (due to arxiv-auth dep)
            # self.assertEqual(response.status_code, status.UNAUTHORIZED,
            #                  "Authentication is required to view status")

            # other = generate_token('1235', 'foo@user.com', 'foouser',
            #                        scope=[scopes.READ_COMPILE,
            #                               scopes.CREATE_COMPILE,
            #                               scopes.CREATE_FULLTEXT,
            #                               scopes.READ_FULLTEXT])
            # response = self.client.get(f'/submission/{self.SUBMISSION_CASE}/status',
            #                            headers={'Authorization': other})
            response = self.client.get(f'/submission/{self.SUBMISSION_CASE}/status')
            self.assertEqual(response.status_code, status.NOT_FOUND,
                             "Not the owner; pretend it does not exist")

        # We should now be able to retrieve the content,
        with self.app.app_context():
            # response = self.client.get(f'/submission/{self.SUBMISSION_CASE}',
            #                            headers={'Authorization': token})
            response = self.client.get(f'/submission/{self.SUBMISSION_CASE}')

        self.assertEqual(response.status_code, status.OK, 'Returns 200 OK')
        self.assertEqual(response.json['status'], 'succeeded', "Succeeded!")
        self.assertIsNotNone(response.json['content'], 'Content is included')
        self.assertIsNotNone(response.json['started'], "Start time is set")
        self.assertIsNotNone(response.json['ended'], "End time is set")
        self.assertIsNone(response.json['exception'], "No exception occurred")
        self.assertEqual(response.json['owner'], self.user_id,
                         "This is a submission; owner is set.")
        self.assertIsNotNone(response.json['task_id'], "Task ID is set")
        self.assertEqual(response.json['version'], self.extractor_version)

        # We should now be able to retrieve the PSV content, as well.
        with self.app.app_context():
            response = self.client.get(
                f'/submission/{self.SUBMISSION_CASE}/format/psv',
                headers={'Authorization': token}
            )

        self.assertEqual(response.status_code, status.OK, 'Returns 200 OK')

        with self.app.app_context():
            # These should also work.
            valid_urls = [
                f'/submission/{self.SUBMISSION_CASE}/version/0.3',
                f'/submission/{self.SUBMISSION_CASE}/version/0.3/format/plain',
                f'/submission/{self.SUBMISSION_CASE}/version/0.3/format/psv'
            ]
            for url in valid_urls:
                response = self.client.get(url,
                                           headers={'Authorization': token})
                self.assertEqual(response.status_code, status.OK,
                                 f'{url} should exist')

            # But not these.
            invalid_urls = [
                f'/submission/{self.SUBMISSION_CASE}/version/0.2',
                f'/submission/{self.SUBMISSION_CASE}/format/magic',
                f'/submission/{self.SUBMISSION_CASE}/version/0.3/format/magic'
            ]
            for url in invalid_urls:
                response = self.client.get(url,
                                           headers={'Authorization': token})
                self.assertEqual(response.status_code, status.NOT_FOUND,
                                 f'{url} should not exist')

        # Verify that authn/z requirements are enforced for content endpoint.
        with self.app.app_context():
            # unauthz = generate_token('1234', 'foo@user.com', 'foouser',
            #                          scope=[scopes.READ_COMPILE,
            #                                 scopes.CREATE_COMPILE,
            #                                 scopes.CREATE_FULLTEXT])
            # response = self.client.get(f'/submission/{self.SUBMISSION_CASE}',
            #                            headers={'Authorization': unauthz})
            response = self.client.get(f'/submission/{self.SUBMISSION_CASE}')
            self.assertEqual(response.status_code, status.FORBIDDEN,
                             "The fulltext:read scope is required for status")

            response = self.client.get(f'/submission/{self.SUBMISSION_CASE}')
            self.assertEqual(response.status_code, status.UNAUTHORIZED,
                             "Authentication is required to view status")

            # other = generate_token('1235', 'foo@user.com', 'foouser',
            #                        scope=[scopes.READ_COMPILE,
            #                               scopes.CREATE_COMPILE,
            #                               scopes.CREATE_FULLTEXT,
            #                               scopes.READ_FULLTEXT])
            # response = self.client.get(f'/submission/{self.SUBMISSION_CASE}',
            #                            headers={'Authorization': other})
            response = self.client.get(f'/submission/{self.SUBMISSION_CASE}')
            self.assertEqual(response.status_code, status.NOT_FOUND,
                             "Not the owner; pretend it does not exist")
