"""Helpers for service modules."""

import io
from typing import Callable, Iterator, Any
from typing_extensions import Literal


class ReadWrapper(io.BytesIO):
    """Wraps a response body streaming iterator to provide ``read()``."""

    def __init__(self, iter_content: Callable[[int], Iterator[bytes]],
                 size: int = 4096) -> None:
        """Initialize the streaming iterator."""
        self._iter_content = iter_content(size)

    def seekable(self) -> Literal[False]:
        """Indicate that this is a non-seekable stream."""
        return False

    def readable(self) -> Literal[True]:
        """Indicate that it *is* a readable stream."""
        return True

    def read(self, *args: Any, **kwargs: Any) -> bytes:
        """
        Read the next chunk of the content stream.

        Arguments are ignored, since the chunk size must be set at the start.
        """
        return next(self._iter_content, b'')