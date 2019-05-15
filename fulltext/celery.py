"""Initialize the Celery application."""

from celery import Celery

from fulltext import celeryconfig

celery_app = Celery('fulltext')
celery_app.config_from_object('fulltext.celeryconfig')
celery_app.autodiscover_tasks(['fulltext'], related_name='extract', force=True)
