"""Run the indexing agent stream processor."""

from arxiv.base.agent import process_stream
from .consumer import FulltextRecordProcessor
from ..factory import create_web_app


def start_agent() -> None:
    """Start the record processor."""
    app = create_web_app()
    with app.app_context():
        process_stream(FulltextRecordProcessor, app.config,
                       extra=dict(config=app.config))


if __name__ == '__main__':
    start_agent()
