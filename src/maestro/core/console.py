"""Contains the logic to have a live status-line during execution."""

from __future__ import annotations

import itertools
import logging
import shutil
import sys
import threading
import time
from typing import TYPE_CHECKING

from maestro.core.execution import WIDTH, coloured_task_name, coloured_text, draw_progressbar, tag, watched

if TYPE_CHECKING:
    from typing import TextIO

# Wipe a line and back to start
_WIPE = "\r\033[2K"
# Framerate, 40 FPS looks best in PyCharm and Windows Consoles imo
_FPS = 40
# Dynamic duration display
_TIME_FORMATS = ((86400, "d"), (3600, "h"), (60, "m"), (1, "s"))


def _elapsed(since: float) -> str:
    """Elapsed time with dynamic time format"""
    tenths = int((time.monotonic() - since) * 10)
    seconds = tenths // 10
    for (size, unit), (under, smaller) in itertools.pairwise(_TIME_FORMATS):
        if seconds >= size:
            return f"{seconds // size}{unit}{seconds % size // under:02d}{smaller}"
    return f"{seconds}.{tenths % 10}s"


class LiveLineConsole(logging.Handler):
    """Logging handler that always displays a live line of what runs. Things that skip, fail, or succeed cause a new
    line.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Initializes the handler.

        Args:
            stream (TextIO | None): Where to write.
        """
        super().__init__()
        self.stream = stream if stream is not None else sys.stdout
        # Force redirected output to be utf-8
        if reconfigure := getattr(self.stream, "reconfigure", None):
            reconfigure(encoding="utf-8")
        self._running: dict[str, tuple[str, float]] = {}
        self._open = False
        self._stopped = threading.Event()
        # Time and animation thread
        self._animation = threading.Thread(target=self._animate, daemon=True)
        if watched():
            self._animation.start()

    def emit(self, record: logging.LogRecord) -> None:
        """Logger behaviour.

        Args:
            record (logging.LogRecord): The record to write.
        """
        try:
            task = getattr(record, "task", "")
            if task and getattr(record, "running", False):
                self._running[task] = getattr(record, "label", task), time.monotonic()
            else:
                ran = self._running.pop(task, None)
                # Only if a task was running is there a runtime to tell user
                runtime = coloured_text(f" {_elapsed(ran[1])}", "38;5;244") if ran else ""
                self._put(self.format(record) + runtime, finalized=True)
            if self._running:
                self._put(self._live_line(), finalized=False)
            self.stream.flush()
        except Exception:  # noqa: BLE001
            self.handleError(record)

    def close(self) -> None:
        """Clean-up."""
        self._stopped.set()
        if self._animation.is_alive():
            self._animation.join()
        if self._open:
            self.stream.write("\n")
            self._open = False
        self.stream.flush()
        super().close()

    def _animate(self) -> None:
        """Animation loop for live line. Acquire lock to never overwrite wrong line."""
        while not self._stopped.wait(1.0 / _FPS):
            self.acquire()
            try:
                if self._running:
                    self._put(self._live_line(), finalized=False)
                    self.stream.flush()
            finally:
                self.release()

    def _put(self, line: str, *, finalized: bool) -> None:
        """If a line was finalized, starts a new line"""
        ending = "\n" if finalized else ""
        self.stream.write(f"{_WIPE if self._open else ''}{line}{ending}")
        self._open = not finalized

    def _live_line(self) -> str:
        """Everything currently running truncated to WIDTH (wrapping breaks the illusion)"""
        marker = draw_progressbar(time.monotonic()) if watched() else tag("RUNNING")
        names = ", ".join(f"{label} {_elapsed(since)}" for label, since in self._running.values())
        room = shutil.get_terminal_size().columns - WIDTH - 2
        return f"{marker} {coloured_task_name(names if len(names) <= room else names[: room - 3] + '...', 'RUNNING')}"
