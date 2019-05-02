"""Mock endpoints for PDFs."""

from flask import Flask, send_file

application = Flask(__name__)


@application.route('/pdf/<arxiv_id>')
def get_a_pdf(arxiv_id):
    """Get a PDF for an announced e-print."""
    return send_file('/opt/arxiv/1702.07336.pdf', mimetype='application/pdf')


@application.route('/compiler/<string:source_id>/<string:checksum>/pdf/product')
def get_a_submission_pdf(source_id, checksum):
    """Get a PDF for a submission from the compiler service."""
    return send_file('/opt/arxiv/1702.07336.pdf', mimetype='application/pdf'), 200, {'ARXIV-OWNER': '1234'}
