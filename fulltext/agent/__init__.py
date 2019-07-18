"""
The fulltext agent is responsible for performing extractions on new papers.

The agent consumes notifications on the ``PDFIsAvailable`` stream. For each
notification, the agent generates an extraction task for the worker to
complete.
"""
from .consumer import FulltextRecordProcessor
