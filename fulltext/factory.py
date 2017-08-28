"""Application factory for fulltext service components."""

from flask import Flask
from fulltext.api import routes


import logging


def create_web_app():
    """Initialize an instance of the web application."""
    app = Flask('fulltext')
    app.config.from_pyfile('config.py')
    app.register_blueprint(routes.blueprint)

    from fulltext.services.store import store
    store.init_app(app)

    return app


def create_process_app():
    """Initialize an instance of the processing application."""
    from fulltext.celery import app
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)
    return app
