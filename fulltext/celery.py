"""Initialize the Celery application."""

from celery import Celery
from flask import Flask
import os
from fulltext import celeryconfig
flask_app = Flask('fulltext')
flask_app.config.from_pyfile('config.py')


class MetaCelery(Celery):
    """Wrapper for the :class:`.Celery` application with ``config``."""

    def __init__(self, *args, **kwargs):
        super(MetaCelery, self).__init__(*args, **kwargs)
        self.config = {}


FULLTEXT_REDIS_ENDPOINT = os.environ.get('FULLTEXT_REDIS_ENDPOINT')
broker_url = "redis://%s/0" % FULLTEXT_REDIS_ENDPOINT
result_backend = "redis://%s/0" % FULLTEXT_REDIS_ENDPOINT
app = MetaCelery('fulltext', results=result_backend, broker=broker_url)
app.config_from_object(celeryconfig)
app.config.update(flask_app.config)
app.autodiscover_tasks(['fulltext.process', 'fulltext.agent'])
app.conf.task_default_queue = 'fulltext-worker'
