"""Application factory for fulltext service components."""

import logging as pylogging
import os
import re
import sys
import time
from typing import Any

from flask import Flask, jsonify, Response
from typing_extensions import Protocol
from werkzeug.exceptions import HTTPException, Forbidden, Unauthorized, \
    BadRequest, MethodNotAllowed, InternalServerError, NotFound
from werkzeug.routing import BaseConverter, ValidationError

from arxiv.base import Base, logging
# from arxiv.users.auth import Auth
# from arxiv.users import auth
from arxiv.base.middleware import wrap, request_logs
from arxiv import vault

from fulltext.services import store, legacy, preview, extractor
# from fulltext.celery import celery_app
from fulltext import routes
from . import extract

logger = logging.getLogger(__name__)


class SubmissionSourceConverter(BaseConverter):
    """Route converter for submission source identifiers."""

    regex = r'[^/][0-9]+/[^/\?#]+'


def create_web_app(for_worker: bool = False) -> Flask:
    """Initialize an instance of the web application."""
    app = Flask('fulltext')
    app.config.from_pyfile('config.py')
    app.url_map.converters['source'] = SubmissionSourceConverter

    if app.config['LOGLEVEL'] < 40:
        # Make sure that boto doesn't spam the logs when we're in debug mode.
        pylogging.getLogger('boto').setLevel(pylogging.ERROR)
        pylogging.getLogger('boto3').setLevel(pylogging.ERROR)
        pylogging.getLogger('botocore').setLevel(pylogging.ERROR)

    Base(app)
    # Auth(app) # TODO: auth disabled
    app.register_blueprint(routes.blueprint)
    store.Storage.current_session().init_app(app)
    legacy.CanonicalPDF.init_app(app)
    preview.PreviewService.init_app(app)

    # TODO: auth disabled
    # middleware = [auth.middleware.AuthMiddleware]
    middleware = []
    if app.config['VAULT_ENABLED']:
        middleware.insert(0, vault.middleware.VaultMiddleware)
    wrap(app, middleware)
    if app.config['VAULT_ENABLED']:
        app.middlewares['VaultMiddleware'].update_secrets({})

    if app.config['WAIT_FOR_SERVICES']:
        time.sleep(app.config['WAIT_ON_STARTUP'])
        with app.app_context():
            wait_for(store.Storage.current_session())
            if app.config['CANONICAL_AWAIT']:
                wait_for(legacy.CanonicalPDF.current_session())
            if app.config['PREVIEW_AWAIT']:
                wait_for(preview.PreviewService.current_session())
            if for_worker:
                wait_for(extractor.do_extraction)
            else:
                wait_for(extract, await_result=True)    # type: ignore
        logger.info('All upstream services are available; ready to start')

    register_error_handlers(app)
    app.celery_app = extract.get_or_create_worker_app(app)
    return app


class IAwaitable(Protocol):
    """An object that provides an ``is_available`` predicate."""

    def is_available(self, **kwargs: Any) -> bool:
        """Check whether an object (e.g. a service) is available."""
        ...


def wait_for(service: IAwaitable, delay: int = 2, **extra: Any) -> None:
    """Wait for a service to become available."""
    if hasattr(service, '__name__'):
        service_name = service.__name__    # type: ignore
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
