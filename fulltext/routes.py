"""Provides the blueprint for the fulltext API."""

from typing import Optional
from flask import request, Blueprint, Response
from werkzeug.exceptions import NotAcceptable, BadRequest
from flask.json import jsonify
from arxiv import status
from arxiv.users import auth
from fulltext import controllers

blueprint = Blueprint('fulltext', __name__, url_prefix='')


def best_match(available, default):
    """Determine best content type given Accept header and available types."""
    if 'Accept' not in request.headers:
        return default
    return request.accept_mimetypes.best_match(available)


@blueprint.route('/status', methods=['GET'])
def ok() -> tuple:
    """Provide current integration status information for health checks."""
    data, code, headers = controllers.service_status()
    return jsonify(data), code, headers


@blueprint.route('/<arxiv:paper_id>', methods=['POST'])
@auth.decorators.scoped(auth.scopes.CREATE_FULLTEXT)
def extract_fulltext(paper_id: str) -> tuple:
    """Handle requests for fulltext extraction."""
    data, code, headers = controllers.extract(paper_id)
    return jsonify(data), code, headers


@blueprint.route('/submission/<paper_id>', methods=['POST'])
@auth.decorators.scoped(auth.scopes.CREATE_FULLTEXT)
def extract_fulltext_from_submission(paper_id: str) -> tuple:
    """Handle requests for fulltext extraction for submissions."""
    data, code, headers = controllers.extract(paper_id, id_type='submission')
    return jsonify(data), code, headers


@blueprint.route('/<arxiv:paper_id>/version/<version>/format/<content_format>',
                 methods=['GET'])
@blueprint.route('/<arxiv:paper_id>/version/<version>', methods=['GET'])
@blueprint.route('/<arxiv:paper_id>/format/<content_format>', methods=['GET'])
@blueprint.route('/<arxiv:paper_id>', methods=['GET'])
@auth.decorators.scoped(auth.scopes.READ_FULLTEXT)
def retrieve(paper_id: str, version: Optional[str] = None,
             content_format: str = "plain") -> tuple:
    """Retrieve full-text content for an arXiv paper."""
    if paper_id is None:
        raise BadRequest('paper_id missing in request')
    available = ['application/json', 'text/plain']
    content_type = best_match(available, 'application/json')
    data, status_code, headers = controllers.retrieve(
        paper_id,
        content_type,
        id_type='arxiv',
        content_format=content_format
    )

    if content_type == 'text/plain':
        response_data = Response(data['content'], content_type='text/plain')
    elif content_type == 'application/json':
        data['content'] = data['content'].decode('utf-8')
        response_data = jsonify(data)
    else:
        raise NotAcceptable('unsupported content type')
    return response_data, status_code, headers


@blueprint.route('/submission/<paper_id>/version/<version>/format/<content_format>', methods=['GET'])
@blueprint.route('/submission/<paper_id>/version/<version>', methods=['GET'])
@blueprint.route('/submission/<paper_id>/format/<content_format>', methods=['GET'])
@blueprint.route('/submission/<paper_id>', methods=['GET'])
@auth.decorators.scoped(auth.scopes.READ_FULLTEXT)
def retrieve_submission(paper_id: str, version: Optional[str] = None,
                        content_format: str = "plain") -> tuple:
    """Retrieve full-text content for an arXiv paper."""
    available = ['application/json', 'text/plain']
    content_type = best_match(available, 'application/json')
    data, status_code, headers = controllers.retrieve(paper_id, content_type,
                                                      id_type='submission')
    if content_type == 'text/plain':
        response_data = Response(data, content_type='text/plain')
    elif content_type == 'application/json':
        data['content'] = data['content'].decode('utf-8')
        response_data = jsonify(data)
    else:
        raise NotAcceptable('unsupported content type')
    return response_data, status_code, headers


@blueprint.route('/<arxiv:paper_id>/version/<version>/status', methods=['GET'])
@blueprint.route('/<arxiv:paper_id>/status', methods=['GET'])
@auth.decorators.scoped(auth.scopes.READ_FULLTEXT)
def task_status(paper_id: str, version: Optional[str] = None) -> tuple:
    """Get the status of a text extraction task."""
    data, code, headers = controllers.get_task_status(paper_id,
                                                      version=version)
    return jsonify(data), code, headers


@blueprint.route('/submission/<arxiv:paper_id>/version/<version>/status',
                 methods=['GET'])
@blueprint.route('/submission/<arxiv:paper_id>/status', methods=['GET'])
@auth.decorators.scoped(auth.scopes.READ_FULLTEXT)
def submission_task_status(paper_id: str, version: Optional[str] = None) \
        -> tuple:
    """Get the status of a text extraction task."""
    data, code, headers = controllers.get_task_status(paper_id,
                                                      id_type='submission',
                                                      version=version)
    return jsonify(data), code, headers
