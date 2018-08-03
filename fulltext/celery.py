"""Initialize the Celery application."""

from celery import Celery

from fulltext import celeryconfig

celery_app = Celery('fulltext',
                    results=celeryconfig.result_backend,
                    backend=celeryconfig.result_backend,
                    result_backend=celeryconfig.result_backend,
                    broker=celeryconfig.broker_url)
celery_app.config_from_object(celeryconfig)
celery_app.autodiscover_tasks(['fulltext'], related_name='extract', force=True)
celery_app.conf.task_default_queue = 'fulltext-worker'
