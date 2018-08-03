"""Application factory for fulltext service components."""

import logging
from flask import Flask
from arxiv.base import Base
from fulltext.celery import celery_app
from fulltext.services import store, pdf


def create_web_app():
    """Initialize an instance of the web application."""
    from fulltext import routes
    app = Flask('fulltext')
    app.config.from_pyfile('config.py')
    # logging.getLogger('boto').setLevel(logging.DEBUG)
    # logging.getLogger('boto3').setLevel(logging.DEBUG)
    # logging.getLogger('botocore').setLevel(logging.DEBUG)
    Base(app)
    app.register_blueprint(routes.blueprint)
    store.init_app(app)
    pdf.init_app(app)

    return app


def create_worker_app():
    """Initialize an instance of the worker application."""
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    flask_app = Flask('fulltext')
    flask_app.config.from_pyfile('config.py')

    store.init_app(flask_app)
    pdf.init_app(flask_app)
    return flask_app
