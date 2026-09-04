"""Small DAG engine to run pipelines. Fully multi-processed, can probably be made into a server at some point."""

from __future__ import annotations

import io
import logging
import os
import sys
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from traceback import format_exception
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from maestro.core.combinatorics import Task

logger = logging.getLogger(__name__)

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


def _run(task: Task) -> None:
    """Runs one task. Needs to be module-level for multiprocessing."""
    task.run()


def _start(executor: ProcessPoolExecutor | None, task: Task) -> Future[None]:
    """Starts one task. If there is no executor, it runs on main thread and returns wrapped as already completed
    Future."""
    if executor:
        return executor.submit(_run, task)
    here: Future[None] = Future()
    try:
        _run(task)
    except Exception as exc:  # noqa: BLE001
        here.set_exception(exc)
    else:
        here.set_result(None)
    return here


def _tasks(task: Task) -> Iterator[Task]:
    """Traverses pipeline."""
    yield task
    for required in task.requires().values():
        yield from _tasks(required)


def _draw_pipeline(task: Task, states: Mapping[str, str]) -> list[str]:
    """Draws a pipeline left to right. Forks start a new line. Tasks are coloured by their final states."""
    name = type(task).__name__
    task_name = coloured_task_name(name, states.get(task.task_id, "BLOCKED"))
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
    failed: Mapping[str, BaseException],
    states: Mapping[str, str],
    success_state: str,
    *,
    tracebacks: bool = False,
) -> Iterator[str]:
    """Streams a pipeline for drawing into a console followed by errors it had, if any."""
    indent = " " * (WIDTH + 1)
    for index, line in enumerate(_draw_pipeline(root, states)):
        yield f"{tag(success_state)} {line}" if index == 0 else f"{indent}{line}"
    for task in {task.task_id: task for task in _tasks(root)}.values():
        if error := failed.get(task.task_id):
            yield f"{tag('FAILED')} {coloured_task_name(type(task).__name__, 'FAILED')}  {type(error).__name__}: {error}"
            for line in "".join(format_exception(error)).splitlines() if tracebacks else ():
                yield indent + line


@dataclass(frozen=True)
class Log:
    """Log of what happened during a build.

    Attributes:
        finished (tuple[Task, ...]): Pipelines passed to build that completed.
        unfinished (tuple[Task, ...]): Pipelines passed to build that did not complete.
        failed (Mapping[str, BaseException]): Traces of failed Tasks.
        states (Mapping[str, str]): State of every task after the engine had its paws on it.
    """

    finished: tuple[Task, ...]
    unfinished: tuple[Task, ...]
    failed: Mapping[str, BaseException]
    states: Mapping[str, str]

    def report(self, *, successes: bool = True, failures: bool = True, trace: bool = False) -> None:
        """Choose what to report on. All False prints only a tally.

        Args:
            successes (bool): Draw the pipelines that finished.
            failures (bool): Draw the pipelines that failed. Per failed Task in Pipeline, print its error.
            trace (bool): Print not just the error, but its full trace.
        """
        total = len(self.finished) + len(self.unfinished)
        tally = f"{len(self.finished)} of {total} pipelines finished"
        half = (78 - len(tally) - 2) // 2
        success_state = "FAILURE" if self.unfinished else "SUCCESS"
        more_than_zero_pipelines_drawn = bool((successes and self.finished) or (failures and self.unfinished))
        logger.info("")
        # If pipelines are actually being drawn, tell user where to find them
        logger.info(
            _coloured_banner_with_centered_text(
                f"{tally}, report below" if more_than_zero_pipelines_drawn else tally, success_state
            )
        )
        if successes and self.finished:
            logger.info(_coloured_banner_with_centered_text("Successes", "SUCCESS", half))
            for root in self.finished:
                for line in _report(root, self.failed, self.states, "SUCCESS"):
                    logger.info(line)
        if failures and self.unfinished:
            logger.error(_coloured_banner_with_centered_text("Failures", "FAILURE", half))
            for root in self.unfinished:
                for line in _report(root, self.failed, self.states, "FAILURE", tracebacks=trace):
                    logger.error(line)
        if more_than_zero_pipelines_drawn:
            # If pipelines are actually being drawn, tell user where to find them
            logger.info(_coloured_banner_with_centered_text(f"{tally}, report above", success_state))


def _execution_graph_max_concurrency(execution_graph: Mapping[str, set[str]]) -> int:
    """Reading the execution graph allows determining the optimal amount of worker processes.

    Args:
        execution_graph (Mapping[str, set[str]]): What every task in the graph requires, by task_id.

    Returns:
        int: The size of the widest layer, one for a graph that is only a chain.
    """
    layer: dict[str, int] = {}
    # There are never more scheduling layers than tasks
    for _ in range(len(execution_graph)):
        layer |= {
            task_id: 1 + max((layer[required] for required in requirements), default=0)
            for task_id, requirements in execution_graph.items()
            if task_id not in layer and requirements <= layer.keys()
        }
    return max(Counter(layer.values()).values(), default=1)


def run_pipelines(tasks: Task | Iterable[Task], workers: int | None = None) -> Log:
    """Runs the given pipelines' execution graph concurrently.

    Parallel execution is non-blocking, every worker acquires new works as soon as something becomes available. Failures
    and the tasks they blocked in turn are collected and reported on.

    Args:
        tasks (Task | Iterable[Task]): Pipelines to run.
        workers (int | None): Number of worker processes, defaults to max concurrency of execution graph.

    Returns:
        Log: Which pipelines finished/failed.

    Raises:
        RuntimeError: If Execution Graph is cyclical.
    """
    from maestro.core.console import LiveLineConsole  # noqa: PLC0415  Console is truly never needed earlier

    # If nobody has intentionally done anything to the logging the live console it is
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[LiveLineConsole()])
    pipeline_roots = list(tasks) if isinstance(tasks, Iterable) else [tasks]
    waiting: dict[str, Task] = {}
    reachable = list(pipeline_roots)
    # Collect all reachable tasks into waiting by traversal
    while reachable:
        task = reachable.pop()
        if task.task_id not in waiting:
            waiting[task.task_id] = task
            reachable.extend(task.requires().values())
    # memoized execution graph
    execution_graph = {task_id: {r.task_id for r in task.requires().values()} for task_id, task in waiting.items()}
    # Spawn optimal amount of workers for graph
    workers = (
        min(os.cpu_count() or 1, _execution_graph_max_concurrency(execution_graph)) if workers is None else workers
    )
    done: set[str] = set()
    failed: dict[str, BaseException] = {}
    states: dict[str, str] = {}
    blocked: dict[str, Task] = {}
    ready: list[Task] = []
    running: dict[Future[None], Task] = {}
    with ExitStack() as pool:
        executor = pool.enter_context(ProcessPoolExecutor(max_workers=workers)) if workers > 1 else None
        while waiting or ready or running:
            # Tasks that needed something that failed are forever blocked
            for task_id in [
                task_id for task_id in waiting if execution_graph[task_id] & (failed.keys() | blocked.keys())
            ]:
                blocked[task_id] = waiting.pop(task_id)
                states[task_id] = "BLOCKED"
            # Tasks are released for further processing if all their needs are fulfilled
            released = [task for task_id, task in waiting.items() if execution_graph[task_id] <= done]
            if waiting and not (released or ready or running):
                msg = (
                    "Execution has stalled, nothing is running and no task has all of its requirements. This can "
                    "only happen if you are shipping a custom identity function or are mutating synthesized "
                    "pipelines."
                )
                raise RuntimeError(msg)
            # Released tasks become ready, unless they are already complete (due to Target being present)
            for task in released:
                del waiting[task.task_id]
                if task.complete():
                    logger.info(
                        "%s %s %s",
                        tag("SKIPPED"),
                        coloured_task_name(task.variant_label, "SKIPPED"),
                        coloured_text("on disk", "38;5;244"),
                        extra={"task": task.task_id},
                    )
                    states[task.task_id] = "SKIPPED"
                    done.add(task.task_id)
                else:
                    ready.append(task)
            # All free workers attempt to grab work whenever it is available
            while ready and len(running) < workers:
                task = ready.pop()
                logger.info(
                    "%s %s",
                    tag("RUNNING"),
                    coloured_task_name(task.variant_label, "RUNNING"),
                    extra={"task": task.task_id, "running": True, "label": task.variant_label},
                )
                running[_start(executor, task)] = task
            # Wait only until ANYTHING at all comes back
            for future in wait(running, return_when=FIRST_COMPLETED).done:
                task = running.pop(future)
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001 tasks failing is specifically allowed
                    logger.error(  # noqa: TRY400  traceback is not actually discarded, saved to Log
                        "%s %s",
                        tag("FAILED"),
                        coloured_task_name(task.variant_label, "FAILED"),
                        extra={"task": task.task_id},
                    )
                    failed[task.task_id] = exc
                    states[task.task_id] = "FAILED"
                else:
                    logger.info(
                        "%s %s",
                        tag("DONE"),
                        coloured_task_name(task.variant_label, "DONE"),
                        extra={"task": task.task_id},
                    )
                    states[task.task_id] = "DONE"
                    done.add(task.task_id)
    # Which pipelines ran and which failed
    return Log(
        tuple(root for root in pipeline_roots if root.task_id in done),
        tuple(root for root in pipeline_roots if root.task_id not in done),
        failed,
        states,
    )
