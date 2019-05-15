"""Application factory for fulltext service components."""

import os
import sys
import logging as pylogging
import re
import time

from typing import Any
from typing_extensions import Protocol
from werkzeug.exceptions import HTTPException, Forbidden, Unauthorized, \
    BadRequest, MethodNotAllowed, InternalServerError, NotFound
from werkzeug.routing import BaseConverter, ValidationError
from flask import Flask, jsonify, Response

from arxiv.base import Base, logging
from arxiv.users.auth import Auth
from arxiv.users import auth
from arxiv.base.middleware import wrap, request_logs

from arxiv import vault

from fulltext.services import store, pdf, compiler, extractor
from fulltext.celery import celery_app
from fulltext import routes
from . import extract

logger = logging.getLogger(__name__)


class SubmissionSourceConverter(BaseConverter):
    """Route converter for submission source identifiers."""

    regex = r'[^/][0-9]+/[^/\?#]+'


def create_web_app() -> Flask:
    """Initialize an instance of the web application."""
    from . import celeryconfig
    app = Flask('fulltext')
    app.config.from_pyfile('config.py')
    celery_app.config_from_object(celeryconfig)
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
            wait_for(store.Storage.current_session())
            wait_for(pdf.CanonicalPDF.current_session())
            wait_for(compiler.Compiler.current_session())
            wait_for(extract, await_result=True)
        logger.info('All upstream services are available; ready to start')

    register_error_handlers(app)
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
            wait_for(store.Storage.current_session())
            wait_for(pdf.CanonicalPDF.current_session())
            wait_for(compiler.Compiler.current_session())
            wait_for(extractor.do_extraction)
        logger.info('All upstream services are available; ready to start')
    return flask_app


class IAwaitable(Protocol):
    """An object that provides an ``is_available`` predicate."""

    def is_available(self) -> bool:
        """Check whether an object (e.g. a service) is available."""
        ...


def wait_for(service: IAwaitable, delay: int = 2, **extra: Any) -> None:
    """Wait for a service to become available."""
    if hasattr(service, '__name__'):
        service_name = service.__name__
    elif hasattr(service, '__class__'):
        service_name = service.__class__.__name__
    else:
        service_name = str(service)

    logger.info('await %s', service_name)
    while not service.is_available(**extra):
        logger.info('service %s is not available; try again', service_name)
        time.sleep(delay)
    logger.info('service %s is available!', service_name)


def register_error_handlers(app: Flask) -> None:
    """Register error handlers for the Flask app."""
    app.errorhandler(Forbidden)(jsonify_exception)
    app.errorhandler(Unauthorized)(jsonify_exception)
    app.errorhandler(BadRequest)(jsonify_exception)
    app.errorhandler(InternalServerError)(jsonify_exception)
    app.errorhandler(NotFound)(jsonify_exception)
    app.errorhandler(MethodNotAllowed)(jsonify_exception)


def jsonify_exception(error: HTTPException) -> Response:
    """Render exceptions as JSON."""
    exc_resp = error.get_response()
    response: Response = jsonify(reason=error.description)
    response.status_code = exc_resp.status_code
    return response
