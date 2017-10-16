"""Application factory for fulltext service components."""

from flask import Flask
from celery import Celery
from fulltext import celeryconfig
import logging


class MetaCelery(Celery):
    """Wrapper for the :class:`.Celery` application with ``config``."""

    def __init__(self, *args, **kwargs):
        """Set an internal ``config`` object."""
        super(MetaCelery, self).__init__(*args, **kwargs)
        self.config = {}


def create_web_app():
    """Initialize an instance of the web application."""
    from fulltext.services.credentials import credentials
    from fulltext.api import routes
    app = Flask('fulltext')
    app.config.from_pyfile('config.py')
    # logging.getLogger('boto').setLevel(logging.DEBUG)
    # logging.getLogger('boto3').setLevel(logging.DEBUG)
    # logging.getLogger('botocore').setLevel(logging.DEBUG)
    if app.config.get('INSTANCE_CREDENTIALS'):
        credentials.init_app(app)
        credentials.session.get_credentials()

    app.register_blueprint(routes.blueprint)

    from fulltext.services.store import store
    store.init_app(app)

    celery = Celery(app.name, results=celeryconfig.result_backend,
                    broker=celeryconfig.broker_url)
    celery.config_from_object(celeryconfig)
    celery.autodiscover_tasks(['fulltext'], related_name='extract', force=True)
    celery.conf.task_default_queue = 'fulltext-worker'
    return app


def create_worker_app():
    """Initialize an instance of the processing application."""
    from fulltext.services.credentials import credentials

    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    flask_app = Flask('fulltext')
    flask_app.config.from_pyfile('config.py')

    app = MetaCelery(flask_app.name, results=celeryconfig.result_backend,
                     broker=celeryconfig.broker_url)
    app.config_from_object(celeryconfig)
    app.config.update(flask_app.config)

    if app.config.get('INSTANCE_CREDENTIALS'):
        credentials.init_app(app)
        credentials.session.get_credentials()

    app.autodiscover_tasks(['fulltext'], related_name='extract', force=True)
    app.conf.task_default_queue = 'fulltext-worker'
    return app
