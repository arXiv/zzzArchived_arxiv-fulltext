"""Application factory for fulltext service components."""

import logging
import re

from werkzeug.routing import BaseConverter, ValidationError
from flask import Flask

from arxiv.base import Base
from arxiv.users.auth import Auth
from arxiv.users import auth
from arxiv.base.middleware import wrap, request_logs
from arxiv import vault
from fulltext.celery import celery_app
from fulltext.services import store, pdf, compiler


class SubmissionSourceConverter(BaseConverter):
    """Route converter for submission source identifiers."""

    regex = '[^/][0-9]+/[^/\?#]+'


def create_web_app() -> Flask:
    """Initialize an instance of the web application."""
    from fulltext import routes
    app = Flask('fulltext')
    app.config.from_pyfile('config.py')
    app.url_map.converters['source'] = SubmissionSourceConverter
    logging.getLogger('boto').setLevel(logging.DEBUG)
    logging.getLogger('boto3').setLevel(logging.DEBUG)
    logging.getLogger('botocore').setLevel(logging.DEBUG)
    Base(app)
    Auth(app)
    app.register_blueprint(routes.blueprint)
    store.Storage.current_session().init_app(app)
    pdf.CanonicalPDF.init_app(app)
    compiler.Compiler.init_app(app)

    middleware = [auth.middleware.AuthMiddleware]
    if app.config['VAULT_ENABLED']:
        middleware.insert(0, vault.middleware.VaultMiddleware)
    wrap(app, middleware)

    return app


def create_worker_app() -> Flask:
    """Initialize an instance of the worker application."""
    logging.getLogger('boto').setLevel(logging.ERROR)
    logging.getLogger('boto3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.ERROR)

    flask_app = Flask('fulltext')
    flask_app.config.from_pyfile('config.py')

    store.Storage.current_session().init_app(flask_app)
    pdf.CanonicalPDF.init_app(flask_app)
    compiler.Compiler.init_app(flask_app)
    return flask_app
