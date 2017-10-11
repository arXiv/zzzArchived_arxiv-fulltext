"""Provides the blueprint for the fulltext API."""

from flask import request, Blueprint
from flask.json import jsonify
from fulltext.api import controllers
from fulltext import status

blueprint = Blueprint('fulltext', __name__, url_prefix='')


@blueprint.route('/status', methods=['GET'])
def ok() -> tuple:
    """Provide current integration status information for health checks."""
    return jsonify({'iam': 'ok'}), status.HTTP_200_OK


@blueprint.route('/fulltext', methods=['POST'])
def extract_fulltext() -> tuple:
    """Handle requests for reference extraction."""
    ec = controllers.ExtractionController()
    data, status, headers = ec.extract(request.get_json(force=True))
    return jsonify(data), status, headers


@blueprint.route('/fulltext/<string:doc_id>', methods=['GET'])
def retrieve(doc_id):
    """Retrieve full-text content for an arXiv paper."""
    response_data, status_code = controllers.retrieve(doc_id)
    return jsonify(response_data), status_code


@blueprint.route('/status/<string:task_id>', methods=['GET'])
def task_status(task_id: str) -> tuple:
    """Get the status of a reference extraction task."""
    data, status, headers = controllers.ExtractionController().status(task_id)
    return jsonify(data), status, headers
