"""All the fancy output into the console, live-line, ascii-drawings, etc."""

from __future__ import annotations

import itertools
import logging
import os
import shutil
import sys
import threading
import time
from traceback import format_exception
from typing import TYPE_CHECKING

from cosy_luigi.utils import traverse_pipeline

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from typing import TextIO

    from cosy_luigi.core.combinatorics import Task

# TASK COLOURS (their monikers)
_TASK_COLOURS = {"RUNNING": "34", "DONE": "32", "SKIPPED": "36", "FAILED": "31", "BLOCKED": "38;5;208"}
# TASK STATUS COLOURS
_TASK_STATE_COLOURS = {
    "RUNNING": "1;94",
    "DONE": "1;92",
    "SKIPPED": "1;96",
    "FAILED": "1;91",
    "SUCCESS": "1;92",
    "FAILURE": "1;91",
}
# WIDTH FOR ALIGNING
WIDTH = max(len(state) for state in _TASK_STATE_COLOURS) + 2


def watched() -> bool:
    """If nobody is watching we don't do fancy output to avoid logs going to file looking wrong."""
    if os.environ.get("NO_COLOR"):
        return False
    return bool(os.environ.keys() & {"FORCE_COLOR", "PYCHARM_HOSTED"} or sys.stdout.isatty())


def coloured_text(text: str, colour: str) -> str:
    """Wraps text with colour."""
    if not watched():
        return text
    return f"\x1b[{colour}m{text}\x1b[0m"


def tag(state: str) -> str:
    """Pads given string with [] to given width for alignment."""
    return coloured_text(f"[{state}]".ljust(WIDTH), _TASK_STATE_COLOURS[state])


def _blip_color_mixer(intensity: float) -> str:
    """Makes a mixed color between dark blue and electric blue."""
    mixed = (round(empty + (full - empty) * intensity) for full, empty in zip((95, 215, 255), (0, 0, 95), strict=True))
    return "38;2;" + ";".join(str(channel) for channel in mixed)


def _smoothstep(x: float) -> float:
    """Classic smoothstep."""
    return x**2 * (3 - 2 * x)


def draw_progressbar(seconds: float) -> str:
    """Draws one frame of the progressbar. Current frame time determines what to draw."""
    # Blip trail light and light-ahead length
    trail_light, light_ahead = 5.0, 1.0
    progressbar_width = WIDTH - 1
    progressbar = "[" + "\u2500" * (progressbar_width - 2) + "]"
    # 2.8s per cycle, head position is relative to where we are in animation cycle
    head = seconds / 2.8 * progressbar_width
    # Calculate lighting
    distances_to_head = ((head - at + light_ahead) % progressbar_width - light_ahead for at in range(progressbar_width))
    intensity_curve = (
        1 - abs(distance) / (light_ahead if distance < 0 else trail_light) for distance in distances_to_head
    )
    # Apply lighting
    lit = (
        coloured_text(glyph, _blip_color_mixer(_smoothstep(max(0.0, intensity))))
        for glyph, intensity in zip(progressbar, intensity_curve, strict=True)
    )
    return "".join(lit) + " "


def coloured_task_name(task: str, state: str) -> str:
    """Returns a task name in the task color appropriate for its state (i.e. muted green, red, etc.)"""
    return coloured_text(task, _TASK_COLOURS[state])


def _draw_pipeline(task: Task, states: Mapping[Task, str]) -> list[str]:
    """Draws a pipeline left to right. Forks start a new line. Tasks are coloured by their final states."""
    name = type(task).__name__
    task_name = coloured_task_name(name, states.get(task, "BLOCKED"))
    bar, link = coloured_text("\u2502", "38;5;244"), coloured_text(" \u2500\u2500 ", "38;5;244")
    required = list(task.requires().values())
    if not required:
        return [task_name]
    # classic traversal
    spine, *branches = (_draw_pipeline(child, states) for child in required)
    # Single requirement needs no new line
    lines = [f"{task_name}{link}{spine[0]}"]
    lines += [(bar if branches else " ") + " " * (len(name) + 3) + line for line in spine[1:]]
    for index, branch in enumerate(branches):
        last = index == len(branches) - 1
        lines.append(coloured_text("\u2514\u2500\u2500 " if last else "\u251c\u2500\u2500 ", "38;5;244") + branch[0])
        lines += [(" " if last else bar) + "   " + line for line in branch[1:]]
    return lines


def _coloured_banner_with_centered_text(text: str, success_state: str, width: int = 78) -> str:
    """Used to make summary and info banners."""
    return coloured_text(f" {text} ".center(width, "="), _TASK_STATE_COLOURS[success_state])


def _report(
    root: Task,
    failed: Mapping[Task, BaseException],
    states: Mapping[Task, str],
    success_state: str,
    *,
    tracebacks: bool = False,
) -> Iterator[str]:
    """Streams a pipeline for drawing into a console followed by errors it had, if any."""
    indent = " " * (WIDTH + 1)
    for index, line in enumerate(_draw_pipeline(root, states)):
        yield f"{tag(success_state)} {line}" if index == 0 else f"{indent}{line}"
    for task in dict.fromkeys(traverse_pipeline(root)):
        if error := failed.get(task):
            yield f"{tag('FAILED')} {coloured_task_name(type(task).__name__, 'FAILED')}  {type(error).__name__}: {error}"
            for line in "".join(format_exception(error)).splitlines() if tracebacks else ():
                yield indent + line


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
        """Starting tasks join live line, finishing ones leave it, everything else gets a new line.

        Args:
            record (logging.LogRecord): The record to write.
        """
        try:
            task = getattr(record, "task", "")
            match getattr(record, "event", None):
                case "running":
                    self._running[task] = getattr(record, "label", task), time.monotonic()
                case "finished":
                    _, since = self._running.pop(task)
                    self._put(self.format(record) + coloured_text(f" {_elapsed(since)}", "38;5;244"), finalized=True)
                case _:
                    self._put(self.format(record), finalized=True)
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
