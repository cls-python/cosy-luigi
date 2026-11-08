"""Contains the targets tasks write their outputs to. A task whose targets are all present is complete."""

from __future__ import annotations

import io
import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Iterator


class Target(ABC):
    """Contains output of a Task. If present, a task is considered already complete.

    Attributes:
        path (str): Output path to write to.
    """

    path: str

    @abstractmethod
    def exists(self) -> bool:
        """Whether the output container is present."""

    @abstractmethod
    def open(self, mode: str = "r") -> Any:
        """Interaction with output container."""

    def __str__(self) -> str:
        return self.path


class LocalTarget(Target):
    """Target that writes files to disk."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        """Initializes LocalTarget.

        Args:
            path (str | os.PathLike[str]): Path to the file.
        """
        self.path = str(path)

    def exists(self) -> bool:
        """Checks if the file is present already."""
        return Path(self.path).exists()

    def open(self, mode: str = "r") -> Any:
        """Opens the file. If writing to it, uses the atomic context manager"""
        return self._atomic_write(mode) if set(mode) & {"w", "a", "x"} else Path(self.path).open(mode)

    @contextmanager
    def _atomic_write(self, mode: str) -> Iterator[Any]:
        """The context manager holds target path but gives holder a temporary file. On success, it is renamed, else
        deleted."""
        final = Path(self.path)
        final.parent.mkdir(parents=True, exist_ok=True)
        partial = final.with_name(f"{final.name}.{os.getpid()}.partial")
        try:
            with partial.open(mode) as handle:
                yield handle
            partial.replace(final)
        finally:
            partial.unlink(missing_ok=True)


class MemoryTarget(Target):
    """Target that writes memory-only for tests/debug. Processes do not share memory, so a pipeline using it must not
    use multiple workers."""

    _written: ClassVar[dict[str, str]] = {}

    def __init__(self, path: str) -> None:
        """Initializes MemoryTarget.

        Args:
            path (str): The path in memory.
        """
        self.path = str(path)

    def exists(self) -> bool:
        """Checks the records of what was 'written' to see if target exists."""
        return self.path in self._written

    def open(self, mode: str = "r") -> Any:
        """Opens the file. If writing to it, uses the atomic context manager"""
        return self._atomic_memory_write() if set(mode) & {"w", "a", "x"} else io.StringIO(self._written[self.path])

    @contextmanager
    def _atomic_memory_write(self) -> Iterator[io.StringIO]:
        """In memory, we don't need to clean-up anything so a plain-jane context manager is fine."""
        buffer = io.StringIO()
        yield buffer
        MemoryTarget._written[self.path] = buffer.getvalue()

    @classmethod
    def clear(cls) -> None:
        """Wipes the memory."""
        cls._written.clear()
