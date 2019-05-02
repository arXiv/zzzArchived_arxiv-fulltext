"""Domain classes for fulltext extraction service."""

from typing import NamedTuple, Optional, Any
from datetime import datetime
from pytz import UTC
from backports.datetime_fromisoformat import MonkeyPatch
from enum import Enum

MonkeyPatch.patch_fromisoformat()


class Extraction(NamedTuple):
    """Metadata about an extraction."""

    class Status(Enum):
        """Task Status."""

        IN_PROGRESS = 'in_progress'
        SUCCEEDED = 'succeeded'
        FAILED = 'failed'

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
    status: 'Extraction.Status' = Status.IN_PROGRESS
    """Status of the extraction task."""
    content: Optional[bytes] = None
    """Extraction content, in the specified :attr:`.format`."""

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
            'content': self.content
        }

    def copy(self, **kwargs) -> 'Extraction':
        """Create a new :class:`.Extraction` with updated values."""
        data = self.to_dict()
        data.update(kwargs)
        if type(data['status']) is str:
            data['status'] = Extraction.Status(data['status'])
        if type(data['started']) is str:
            data['started'] = datetime.fromisoformat(data['started'])
        if type(data['ended']) is str:
            data['ended'] = datetime.fromisoformat(data['ended'])
        return Extraction(**data)

    @property
    def completed(self):
        """Determine whether the task is in a completed states."""
        return self.status in [self.Status.SUCCEEDED, self.Status.FAILED]
