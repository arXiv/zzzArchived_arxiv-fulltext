"""Provides the blueprint for the fulltext API."""

from typing import Optional
from flask import request, Blueprint, Response
from flask.json import jsonify
from arxiv import status
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
    return jsonify({'iam': 'ok'}), status.HTTP_200_OK


@blueprint.route('/status/<task_id>', methods=['GET'])
def task_status(task_id: str) -> tuple:
    """Get the status of a reference extraction task."""
    data, code, headers = controllers.get_task_status(task_id)
    return jsonify(data), code, headers


@blueprint.route('/<arxiv:paper_id>', methods=['POST'])
@blueprint.route('/<id_type>/<paper_id>', methods=['POST'])
def extract_fulltext(paper_id: str, id_type: str = 'arxiv') -> tuple:
    """Handle requests for reference extraction."""
    data, code, headers = controllers.extract(paper_id)
    return jsonify(data), code, headers


@blueprint.route('/<arxiv:paper_id>/version/<version>/format/<content_format>', methods=['GET'])
@blueprint.route('/<arxiv:paper_id>/version/<version>', methods=['GET'])
@blueprint.route('/<arxiv:paper_id>/format/<content_format>', methods=['GET'])
@blueprint.route('/<arxiv:paper_id>', methods=['GET'])
def retrieve(paper_id: str, version: Optional[str] = None,
             content_format: str = "plain") -> tuple:
    """Retrieve full-text content for an arXiv paper."""
    available = ['application/json', 'text/plain']
    content_type = best_match(available, 'application/json')
    data, status_code, headers = controllers.retrieve(paper_id, content_type,
                                                      id_type='arxiv')
    if content_type == 'text/plain':
        response_data = Response(data, content_type='text/plain')
    elif content_type == 'application/json':
        response_data = jsonify(data)
    return response_data, status_code, headers


@blueprint.route('/submission/<paper_id>/version/<version>/format/<content_format>', methods=['GET'])
@blueprint.route('/submission/<paper_id>/version/<version>', methods=['GET'])
@blueprint.route('/submission/<paper_id>/format/<content_format>', methods=['GET'])
@blueprint.route('/submission/<paper_id>', methods=['GET'])
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
        response_data = jsonify(data)
    return response_data, status_code, headers
