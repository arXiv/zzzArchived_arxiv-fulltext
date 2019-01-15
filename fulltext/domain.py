"""Domain classes for fulltext extraction service."""

from typing import NamedTuple, Optional
from datetime import datetime
from enum import Enum


class ExtractionProduct(NamedTuple):
    """Represents a plaintext extraction."""

    paper_id: str
    """Identifier of the document from which the extraction was generated."""
    content: bytes
    """Extraction content, in the specified :attr:`.format`."""
    version: str
    """The version of the extractor that generated the product."""
    format: str
    """The format of the extraction :attr:`content`. Usually ``plain``."""
    created: datetime
    """The datetime when the extraction was created."""

    def to_dict(self) -> dict:
        """Generate a dict representation of this extraction product."""
        return {
            'paper_id': self.paper_id,
            'content': self.content,
            'version': self.version,
            'format': self.format,
            'created': self.created.isoformat()
        }


class ExtractionPlaceholder(NamedTuple):
    """Represents a placeholder for an in-progress extraction."""

    task_id: Optional[str] = None
    """The identifier of the running task."""

    exception: Optional[str] = None
    """An exception raised during a failed task."""

    content: None = None

    def to_dict(self) -> dict:
        """Generate a dict representation of this placeholder."""
        return {
            'task_id': self.task_id,
            'exception': self.exception,
            'content': self.content
        }


class ExtractionTask(NamedTuple):
    """Represents an extraction task."""

    class Statuses(Enum):
        """Task statuses."""

        IN_PROGRESS = 'in_progress'
        SUCCEEDED = 'succeeded'
        FAILED = 'failed'

    task_id: str
    status: 'ExtractionTask.Status'
    paper_id: Optional[str] = None
    """Identifier of the document from which the extraction was generated."""
    id_type: Optional[str] = None
    result: Optional[str] = None

    def to_dict(self) -> dict:
        """Generate a dict representation of this task."""
        return {
            'task_id': self.task_id,
            'status': self.status,
            'paper_id': self.paper_id,
            'id_type': self.id_type
        }
