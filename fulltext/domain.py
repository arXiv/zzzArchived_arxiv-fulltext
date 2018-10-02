"""Domain classes for fulltext extraction service."""

from typing import NamedTuple
from datetime import datetime


class ExtractionProduct(NamedTuple):
    """Represents a plaintext extraction."""

    paper_id: str
    """Identifier of the document from which the extraction was generated."""
    content: str
    """Extraction content, in the specified :attr:`.format`."""
    version: str
    """The version of the extractor that generated the product."""
    format: str
    """The format of the extraction :attr:`content`. Usually ``plain``."""
    etag: str
    """MD5 checksum of the extraction content."""
    created: datetime
    """The datetime when the extraction was created."""
