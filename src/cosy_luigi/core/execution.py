"""Small DAG engine to run pipelines. Fully multi-processed, can probably be made into a server at some point."""

from __future__ import annotations

import logging
import os
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from contextlib import ExitStack
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from cosy_luigi.core.console import (
    LiveLineConsole,
    _coloured_banner_with_centered_text,
    _report,
    coloured_task_name,
    coloured_text,
    tag,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cosy_luigi.core.combinatorics import Task

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class Log:
    """Log of what happened during a build.

    Attributes:
        finished (tuple[Task, ...]): Pipelines passed to build that completed.
        unfinished (tuple[Task, ...]): Pipelines passed to build that did not complete.
        failed (Mapping[Task, BaseException]): Traces of failed Tasks.
        states (Mapping[Task, str]): State of every task after the engine had its paws on it.
    """

    finished: tuple[Task, ...]
    unfinished: tuple[Task, ...]
    failed: Mapping[Task, BaseException]
    states: Mapping[Task, str]

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


def _execution_graph_max_concurrency(execution_graph: Mapping[Task, set[Task]]) -> int:
    """Reading the execution graph allows determining the optimal amount of worker processes.

    Args:
        execution_graph (Mapping[Task, set[Task]]): What every task in the graph requires.

    Returns:
        int: The size of the widest layer, one for a graph that is only a chain.
    """
    layer: dict[Task, int] = {}
    # Smallest first means a requirement always comes before it is, well, required
    for task in sorted(execution_graph):
        if execution_graph[task] <= layer.keys():
            layer[task] = 1 + max((layer[required] for required in execution_graph[task]), default=0)
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
    # If nobody has intentionally done anything to the logging the live console it is
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[LiveLineConsole()])
    pipeline_roots = list(tasks) if isinstance(tasks, Iterable) else [tasks]
    execution_graph: dict[Task, set[Task]] = {}
    reachable = list(pipeline_roots)
    # Collect all reachable tasks mapped to their needs by traversal
    while reachable:
        task = reachable.pop()
        if task not in execution_graph:
            execution_graph[task] = set(needed := task.requires().values())
            reachable.extend(needed)
    # reverse, all upstream consumers
    consumers: dict[Task, list[Task]] = {}
    for task, requirements in execution_graph.items():
        for required in requirements:
            consumers.setdefault(required, []).append(task)
    # Spawn optimal amount of workers for graph
    workers = (
        min(os.cpu_count() or 1, _execution_graph_max_concurrency(execution_graph)) if workers is None else workers
    )
    # Initially all tasks are waiting by definition
    waiting = dict(execution_graph)
    done: set[Task] = set()
    failed: dict[Task, BaseException] = {}
    states: dict[Task, str] = {}
    ready: list[Task] = []
    running: dict[Future[None], Task] = {}
    with ExitStack() as pool:
        executor = pool.enter_context(ProcessPoolExecutor(max_workers=workers)) if workers > 1 else None
        while waiting or ready or running:
            # Tasks are released for further processing if all their needs are fulfilled
            released = [task for task, needs in waiting.items() if needs <= done]
            if waiting and not (released or ready or running):
                msg = (
                    "Execution has stalled, nothing is running and no task has all of its requirements. This can "
                    "only happen if you are shipping a custom identity function or are mutating synthesized "
                    "pipelines."
                )
                raise RuntimeError(msg)
            # Released tasks become ready, unless they are already complete (due to Target being present)
            for task in released:
                del waiting[task]
                if task.complete():
                    logger.info(
                        "%s %s %s",
                        tag("SKIPPED"),
                        coloured_task_name(task.variant_label, "SKIPPED"),
                        coloured_text("on disk", "38;5;244"),
                        extra={"task": task.task_id},
                    )
                    states[task] = "SKIPPED"
                    done.add(task)
                else:
                    ready.append(task)
            # All free workers attempt to grab work whenever it is available
            while ready and len(running) < workers:
                task = ready.pop()
                logger.info(
                    "%s %s",
                    tag("RUNNING"),
                    coloured_task_name(task.variant_label, "RUNNING"),
                    extra={"task": task.task_id, "event": "running", "label": task.variant_label},
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
                        extra={"task": task.task_id, "event": "finished"},
                    )
                    failed[task] = exc
                    states[task] = "FAILED"
                    # Anything depending on a task is blocked forever if the task failed
                    above = list(consumers.get(task, ()))
                    while above:
                        if (consumer := above.pop()) in waiting:
                            del waiting[consumer]
                            states[consumer] = "BLOCKED"
                            above += consumers.get(consumer, ())
                else:
                    logger.info(
                        "%s %s",
                        tag("DONE"),
                        coloured_task_name(task.variant_label, "DONE"),
                        extra={"task": task.task_id, "event": "finished"},
                    )
                    states[task] = "DONE"
                    done.add(task)
    # Which pipelines ran and which failed
    return Log(
        tuple(root for root in pipeline_roots if root in done),
        tuple(root for root in pipeline_roots if root not in done),
        MappingProxyType(failed),
        MappingProxyType(states),
    )
