from fulltext.services.store import store
from fulltext.logging import getLogger
HTTP_200_OK = 200
HTTP_404_NOT_FOUND = 404
HTTP_500_INTERNAL_SERVER_ERROR = 500

logger = getLogger(__name__)


def retrieve(document_id: str) -> tuple:
    """
    Handle request for full-text content for an arXiv paper.

    Parameters
    ----------
    document_id : str

    Returns
    -------
    tuple
    """
    try:
        content_data = store.session.latest(document_id)
    except IOError as e:
        logger.error(str(e))
        return {
            'explanation': 'Could not connect to data source'
        }, HTTP_500_INTERNAL_SERVER_ERROR
    except Exception as e:
        return {'explanation': str(e)}, HTTP_500_INTERNAL_SERVER_ERROR
    if content_data is None:
        return {
            'explanation': 'fulltext not available for %s' % document_id
        }, HTTP_404_NOT_FOUND
    return content_data, HTTP_200_OK
