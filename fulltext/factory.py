"""Application factory for fulltext service components."""

import logging
from flask import Flask
from celery import Celery
from fulltext import celeryconfig
from fulltext.services import store, retrieve, fulltext, metrics

celery_app = Celery(__name__, results=celeryconfig.result_backend,
                    broker=celeryconfig.broker_url)
celery_app.config_from_object(celeryconfig)
celery_app.autodiscover_tasks(['fulltext'], related_name='extract', force=True)
celery_app.conf.task_default_queue = 'fulltext-worker'


def create_web_app():
    """Initialize an instance of the web application."""
    from fulltext.services import credentials
    from fulltext import api
    app = Flask('fulltext')
    app.config.from_pyfile('config.py')
    # logging.getLogger('boto').setLevel(logging.DEBUG)
    # logging.getLogger('boto3').setLevel(logging.DEBUG)
    # logging.getLogger('botocore').setLevel(logging.DEBUG)
    credentials.init_app(app)
    credentials.get_credentials()

    app.register_blueprint(api.blueprint)
    store.init_app(app)
    retrieve.init_app(app)
    fulltext.init_app(app)
    metrics.init_app(app)

    celery = Celery(app.name, results=celeryconfig.result_backend,
                    broker=celeryconfig.broker_url)
    celery.config_from_object(celeryconfig)
    celery.autodiscover_tasks(['fulltext'], related_name='extract', force=True)
    celery.conf.task_default_queue = 'fulltext-worker'
    return app


def create_worker_app():
    """Initialize an instance of the processing application."""
    from fulltext.services import credentials

    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    flask_app = Flask('fulltext')
    flask_app.config.from_pyfile('config.py')

    celery_app.conf.update(flask_app.config)

    credentials.init_app(flask_app)
    credentials.get_credentials()

    store.init_app(flask_app)
    retrieve.init_app(flask_app)
    fulltext.init_app(flask_app)
    metrics.init_app(flask_app)
    return flask_app
