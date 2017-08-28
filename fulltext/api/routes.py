from flask import request, Blueprint
from flask.json import jsonify
from fulltext.api import controllers

blueprint = Blueprint('fulltext_api', __name__, url_prefix='')


@blueprint.route('/fulltext/<string:document_id>', methods=['GET'])
def retrieve(document_id):
    """Retrieve full-text content for an arXiv paper."""
    response_data, status_code = controllers.retrieve(document_id)
    return jsonify(response_data), status_code
