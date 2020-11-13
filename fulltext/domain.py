"""Domain classes for fulltext extraction service."""

from typing import NamedTuple, Optional, Any
from datetime import datetime
from pytz import UTC
from backports.datetime_fromisoformat import MonkeyPatch
from enum import Enum

MonkeyPatch.patch_fromisoformat()


class Status(Enum):
    """Task Status."""

    IN_PROGRESS: str = 'in_progress'
    SUCCEEDED: str = 'succeeded'
    FAILED: str = 'failed'

class Extraction(NamedTuple):    # arch: domain
    """Metadata about an extraction."""

    identifier: str
    """Identifier of the document from which the extraction was generated."""
    version: str
    """The version of the extractor that generated the product."""
    bucket: str = 'arxiv'
    """The bucket or collection to which the extraction belongs."""
    started: Optional[datetime] = None
    """The datetime when the extraction was created."""
    ended: Optional[datetime] = None
    """The datetime when the extraction was completed."""
    owner: Optional[str] = None
    """Owner of the resource."""
    exception: Optional[str] = None
    """An exception raised during a failed task."""
    task_id: Optional[str] = None
    """The identifier of the running task."""
    status: Status = Status.IN_PROGRESS
    """Status of the extraction task."""
    content: Optional[str] = None
    """Extraction content."""

    def to_dict(self) -> dict:
        """Generate a dict representation of this placeholder."""
        return {
            'identifier': self.identifier,
            'version': self.version,
            'started': self.started.isoformat() if self.started else None,
            'ended': self.ended.isoformat() if self.ended else None,
            'owner': self.owner,
            'task_id': self.task_id,
            'exception': self.exception,
            'status': self.status.value,
            'content': self.content,
            'bucket': self.bucket
        }

    def copy(self, **kwargs: Any) -> 'Extraction':
        """Create a new :class:`.Extraction` with updated values."""
        data = self.to_dict()
        data.update(kwargs)
        # mypy does not know about fromisoformat yet, apparently.
        if isinstance(data['status'], str):
            data['status'] = Status(data['status'])
        if isinstance(data['started'], str):
            data['started'] = datetime.fromisoformat(data['started'])
        if isinstance(data['ended'], str):
            data['ended'] = datetime.fromisoformat(data['ended'])
        return Extraction(**data)

    @property
    def completed(self) -> bool:
        """Determine whether the task is in a completed states."""
        return self.status in [Status.SUCCEEDED, Status.FAILED]


class _SupportedFormats:
    """Defines the text output formats supported by this service."""

    PLAIN = 'plain'
    PSV = 'psv'

    def __contains__(self, value: str) -> bool:
        return value in [self.PLAIN, self.PSV]


class _SupportedBuckets:
    """Defines the supported buckets for extracted plain text."""

    ARXIV = 'arxiv'
    SUBMISSION = 'submission'

    def __contains__(self, value: str) -> bool:
        return value in [self.ARXIV, self.SUBMISSION]


SupportedFormats = _SupportedFormats()      # arch: domain
SupportedBuckets = _SupportedBuckets()      # arch: domain

SupportedFormats.PLAIN
