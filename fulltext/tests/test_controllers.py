"""Tests for :mod:`fulltext.controllers`."""

from unittest import TestCase, mock
from datetime import datetime
from werkzeug.exceptions import InternalServerError, NotFound, BadRequest
from arxiv import status
from .. import controllers, domain, extract
from ..services import store
from ..domain import ExtractionTask


class TestServiceStatus(TestCase):
    """Test service status controller."""

    @mock.patch(f'{controllers.__name__}.store')
    def test_service_is_ready(self, mock_store):
        """The connection to the content store is g2g."""
        mock_store.ready.return_value = True
        data, code, headers = controllers.service_status()
        self.assertEqual(code, status.HTTP_200_OK)

    @mock.patch(f'{controllers.__name__}.store')
    def test_service_is_not_ready(self, mock_store):
        """The connection to the content store is g2g."""
        mock_store.ready.return_value = False
        with self.assertRaises(InternalServerError):
            controllers.service_status()


class TestRetrieve(TestCase):
    """Test retrieving fulltext extractions."""

    @mock.patch(f'{controllers.__name__}.store.retrieve')
    def test_extraction_exists(self, mock_retrieve):
        """The requested extraction exists."""
        version = '0.3'
        paper_id = '1234.56789v2'
        mock_retrieve.return_value = domain.ExtractionProduct(
            paper_id=paper_id,
            version=version,
            format='plain',
            content='foocontent',
            created=datetime.now()
        )
        data, code, headers = controllers.retrieve(paper_id)
        self.assertEqual(code, status.HTTP_200_OK)

    @mock.patch(f'{controllers.__name__}.extraction_task_exists')
    @mock.patch(f'{controllers.__name__}.store.retrieve')
    def test_extraction_does_not_exist(self, mock_retrieve, mock_exists):
        """The requested extraction does not exist."""
        paper_id = '1234.56789v2'
        mock_exists.return_value = False
        mock_retrieve.side_effect = store.DoesNotExist
        with self.assertRaises(NotFound):
            controllers.retrieve(paper_id)

    @mock.patch(f'{controllers.__name__}.url_for')
    @mock.patch(f'{controllers.__name__}.extraction_task_exists')
    @mock.patch(f'{controllers.__name__}.store.retrieve')
    def test_in_progress(self, mock_retrieve, mock_exists, mock_url_for):
        """The requested extraction is in progress."""
        mock_url_for.return_value = '/path/to/task/status'
        paper_id = '1234.56789v2'
        mock_exists.return_value = True
        mock_retrieve.side_effect = store.DoesNotExist
        data, code, headers = controllers.retrieve(paper_id)
        self.assertEqual(code, status.HTTP_303_SEE_OTHER)
        self.assertEqual(headers['Location'], '/path/to/task/status')


class TestExtract(TestCase):
    """Test requesting a new extraction."""

    @mock.patch(f'{controllers.__name__}.extraction_task_exists')
    @mock.patch(f'{controllers.__name__}.create_extraction_task')
    @mock.patch(f'{controllers.__name__}.url_for')
    @mock.patch(f'{controllers.__name__}.pdf.exists')
    def test_arxiv_paper_exists(self, mock_exists, mock_url_for,
                                mock_create, mock_task_exists):
        """Request extraction for an existant arXiv paper."""
        paper_id = '1234.56789v2'
        task_id = extract.task_id(paper_id, 'arxiv'),
        mock_exists.return_value = True
        mock_task_exists.return_value = False
        mock_url_for.side_effect = [
            lambda *a, **k: f'https://arxiv.org/pdf/{k["paper_id"]}',
            lambda *a, **k: f'/fulltext/status/{k["task_id"]}'
        ]
        mock_create.return_value = paper_id

        data, code, headers = controllers.extract(paper_id)
        self.assertEqual(code, status.HTTP_202_ACCEPTED)
        self.assertIn('Location', headers)

    @mock.patch(f'{controllers.__name__}.extraction_task_exists')
    @mock.patch(f'{controllers.__name__}.url_for')
    @mock.patch(f'{controllers.__name__}.pdf.exists')
    def test_task_exists(self, mock_exists, mock_url_for, mock_task_exists):
        """Request extraction for an existant arXiv paper."""
        paper_id = '1234.56789v2'
        task_id = extract.task_id(paper_id, 'arxiv'),
        mock_exists.return_value = True
        mock_task_exists.return_value = True
        mock_url_for.side_effect = [
            lambda *a, **k: f'https://arxiv.org/pdf/{k["paper_id"]}',
            lambda *a, **k: f'/fulltext/status/{k["task_id"]}'
        ]

        data, code, headers = controllers.extract(paper_id)
        self.assertEqual(code, status.HTTP_303_SEE_OTHER)
        self.assertIn('Location', headers)

    @mock.patch(f'{controllers.__name__}.extraction_task_exists')
    @mock.patch(f'{controllers.__name__}.url_for')
    @mock.patch(f'{controllers.__name__}.pdf.exists')
    def test_arxiv_paper_does_not_exist(self, mock_exists, mock_url_for,
                                        mock_task_exists):
        """Request extraction for a non-existant arXiv paper."""
        mock_task_exists.return_value = False
        paper_id = '1234.56789v2'
        mock_exists.return_value = False
        mock_url_for.side_effect = \
            lambda *a, **k: f'https://arxiv.org/pdf/{k["paper_id"]}'

        with self.assertRaises(NotFound):
            controllers.extract(paper_id)

    @mock.patch(f'{controllers.__name__}.extraction_task_exists')
    @mock.patch(f'{controllers.__name__}.url_for')
    @mock.patch(f'{controllers.__name__}.pdf.exists')
    def test_arxiv_submission_does_not_exist(self, mock_exists, mock_url_for,
                                             mock_task_exists):
        """Request extraction for a non-existant submission."""
        mock_task_exists.return_value = False
        paper_id = '1234.56789v2'
        mock_exists.return_value = False
        mock_url_for.side_effect = \
            lambda *a, **k: f'https://arxiv.org/upload/{k["submission_id"]}'

        with self.assertRaises(NotFound):
            controllers.extract(paper_id, id_type='submission')

    def test_weird_identifier(self):
        """Request extraction for a weird ID."""
        paper_id = '1234.56789v2'

        with self.assertRaises(NotFound):
            controllers.extract(paper_id, id_type='somethingfishy')

    @mock.patch(f'{controllers.__name__}.extraction_task_exists')
    @mock.patch(f'{controllers.__name__}.url_for')
    @mock.patch(f'{controllers.__name__}.pdf.exists')
    @mock.patch(f'{controllers.__name__}.create_extraction_task')
    def test_creation_failed(self, mock_create, mock_exists, mock_url_for,
                             mock_task_exists):
        """Could not create an extraction task."""
        paper_id = '1234.56789v2'
        mock_task_exists.return_value = False
        mock_url_for.side_effect = \
            lambda *a, **k: f'https://arxiv.org/pdf/{k["paper_id"]}'
        mock_exists.return_value = True
        mock_create.side_effect = extract.TaskCreationFailed
        with self.assertRaises(InternalServerError):
            controllers.extract(paper_id, id_type='arxiv')


class TestTaskStatus(TestCase):
    """Test getting the status of a task."""

    @mock.patch(f'{controllers.__name__}.get_extraction_task')
    def test_task_is_pending(self, mock_get_task):
        """Pending status is the default; we set to SENT on creation."""
        mock_get_task.side_effect = extract.NoSuchTask
        with self.assertRaises(NotFound):
            controllers.get_task_status('1234.56789')

    @mock.patch(f'{controllers.__name__}.get_extraction_task')
    def test_task_is_in_progress(self, mock_get_task):
        """The task is currently running."""
        paper_id = '1234.56789'
        mock_get_task.return_value = ExtractionTask(
            task_id=extract.task_id(paper_id, 'arxiv'),
            status=ExtractionTask.Statuses.IN_PROGRESS
        )
        data, code, headers = controllers.get_task_status(paper_id)
        self.assertEqual(code, status.HTTP_200_OK)

    @mock.patch(f'{controllers.__name__}.get_extraction_task')
    def test_task_failed(self, mock_get_task):
        """Task failed for good."""
        paper_id = '1234.56789'
        mock_get_task.return_value = ExtractionTask(
            task_id=extract.task_id(paper_id, 'arxiv'),
            status=ExtractionTask.Statuses.FAILED,
            result='No good!'
        )
        data, code, headers = controllers.get_task_status(paper_id)
        self.assertEqual(code, status.HTTP_200_OK)
        self.assertEqual(data['reason'], 'No good!')

    @mock.patch(f'{controllers.__name__}.url_for')
    @mock.patch(f'{controllers.__name__}.get_extraction_task')
    def test_task_succeeded(self, mock_get_task, mock_url_for):
        """Task succeeded."""
        paper_id = '1234.56789'
        mock_get_task.return_value = ExtractionTask(
            task_id=extract.task_id(paper_id, 'arxiv'),
            status=ExtractionTask.Statuses.SUCCEEDED,
            paper_id=paper_id,
            id_type='arxiv'
        )
        mock_url_for.side_effect = lambda *a, **k: '/fulltext/{k["paper_id"]}'
        data, code, headers = controllers.get_task_status(paper_id)
        self.assertEqual(code, status.HTTP_303_SEE_OTHER)
        self.assertIn('Location', headers)

    @mock.patch(f'{controllers.__name__}.url_for')
    @mock.patch(f'{controllers.__name__}.get_extraction_task')
    def test_sub_task_succeeded(self, mock_get_task, mock_url_for):
        """Task succeeded."""
        paper_id = '1234'
        mock_get_task.return_value = ExtractionTask(
            task_id=extract.task_id(paper_id, 'arxiv'),
            status=ExtractionTask.Statuses.SUCCEEDED,
            paper_id=paper_id,
            id_type='submission'
        )
        mock_url_for.side_effect = \
            lambda *a, **k: '/fulltext/submission/{k["paper_id"]}'
        data, code, headers = controllers.get_task_status(paper_id)
        self.assertEqual(code, status.HTTP_303_SEE_OTHER)
        self.assertIn('Location', headers)

    @mock.patch(f'{controllers.__name__}.get_extraction_task')
    def test_task_succeeded_oddly(self, mock_get_task):
        """Task succeeded, but the identifier is mangled..."""
        paper_id = '1234'
        mock_get_task.return_value = ExtractionTask(
            task_id=extract.task_id(paper_id, 'arxiv'),
            status=ExtractionTask.Statuses.SUCCEEDED,
            paper_id=paper_id,
            id_type='somethingfishy'
        )
        with self.assertRaises(NotFound):
            controllers.get_task_status(paper_id)
