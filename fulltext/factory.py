"""Application factory for fulltext service components."""

import os
import sys
import logging as pylogging
import re
import time

from typing_extensions import Protocol
from werkzeug.routing import BaseConverter, ValidationError
from flask import Flask

from arxiv.base import Base, logging
from arxiv.users.auth import Auth
from arxiv.users import auth
from arxiv.base.middleware import wrap, request_logs

from arxiv import vault

from fulltext.celery import celery_app
from fulltext.services import store, pdf, compiler, extractor
from . import extract

logger = logging.getLogger(__name__)


class SubmissionSourceConverter(BaseConverter):
    """Route converter for submission source identifiers."""

    regex = '[^/][0-9]+/[^/\?#]+'


def create_web_app() -> Flask:
    """Initialize an instance of the web application."""
    from fulltext import routes
    app = Flask('fulltext')
    app.config.from_pyfile('config.py')
    app.url_map.converters['source'] = SubmissionSourceConverter
    pylogging.getLogger('boto').setLevel(pylogging.DEBUG)
    pylogging.getLogger('boto3').setLevel(pylogging.DEBUG)
    pylogging.getLogger('botocore').setLevel(pylogging.DEBUG)
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

    if app.config['WAIT_FOR_SERVICES']:
        time.sleep(app.config['WAIT_ON_STARTUP'])
        with app.app_context():
            wait_for_service(store.Storage.current_session())
            wait_for_service(pdf.CanonicalPDF.current_session())
            wait_for_service(compiler.Compiler.current_session())
            wait_for_service(extract)
    return app


def create_worker_app() -> Flask:
    """Initialize an instance of the worker application."""
    pylogging.getLogger('boto').setLevel(pylogging.ERROR)
    pylogging.getLogger('boto3').setLevel(pylogging.ERROR)
    pylogging.getLogger('botocore').setLevel(pylogging.ERROR)

    flask_app = Flask('fulltext')
    flask_app.config.from_pyfile('config.py')

    store.Storage.current_session().init_app(flask_app)
    pdf.CanonicalPDF.init_app(flask_app)
    compiler.Compiler.init_app(flask_app)

    if flask_app.config['WAIT_FOR_SERVICES']:
        time.sleep(flask_app.config['WAIT_ON_STARTUP'])
        with flask_app.app_context():
            wait_for_service(store.Storage.current_session())
            wait_for_service(pdf.CanonicalPDF.current_session())
            wait_for_service(compiler.Compiler.current_session())
            wait_for_service(extractor.do_extraction)
    return flask_app


class IAwaitable(Protocol):
    """An object that provides an ``is_available`` predicate."""

    def is_available(self) -> bool:
        """Check whether an object (e.g. a service) is available."""
        ...


def wait_for_service(service: IAwaitable, delay: int = 2) -> None:
    """Wait for a service to become available."""
    if hasattr(service, '__name__'):
        service_name = service.__name__
    elif hasattr(service, '__class__'):
        service_name = service.__class__.__name__
    else:
        service_name = str(service)

    logger.info('await %s', service_name)
    while not service.is_available():
        logger.info('service %s is not available; try again', service_name)
        time.sleep(delay)
    logger.info('service %s is available!', service_name)
