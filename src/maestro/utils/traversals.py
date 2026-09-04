"""Contains helper methods centered around traversing collections or pipelines in ways specific to the Maestro."""

from __future__ import annotations

import inspect
from abc import ABC
from collections.abc import Iterable
from typing import TYPE_CHECKING

from maestro import Task

if TYPE_CHECKING:
    from collections.abc import Iterator


def _is_abstract(task: type[Task]) -> bool:
    """A class be abstract in two ways, either in the way Python checks for it or someone explicitly marked it."""
    return inspect.isabstract(task) or ABC in task.__bases__


def flatten(*heterogeneous_task_collection: type[Task] | Iterable[type[Task]]) -> Iterator[type[Task]]:
    """Takes an arbitrarily nested Sequence where the leaves of the nested structure are Tasks' types and flattens it.
    During flattening, if an abstract Task type is encountered, it is instead expanded into the set of its implementing
    subclasses and becomes part of the flattening procedure, i.e. also multiple levels of abstraction are correctly
    handled. See the corresponding test_abstract_variant_expansion.py for an example.

    Args:
        *heterogeneous_task_collection (type[Task] | Sequence[type[Task]]): An arbitrarily nested Sequence where leaves are Tasks' types.

    Yields:
         type[Task]: Every concrete task the collection holds, in the order it was given.
    """
    for task_or_task_collection in heterogeneous_task_collection:
        if isinstance(task_or_task_collection, Iterable):
            yield from flatten(*task_or_task_collection)
        elif _is_abstract(task_or_task_collection):
            # An abstract task expands to its inheritors, which could be abstract and expand again
            yield from (variant for variant in task_or_task_collection.get_all_variants() if not _is_abstract(variant))
        else:
            yield task_or_task_collection


def traverse_pipeline(to_traverse: Task | Iterable[Task]) -> Iterator[Task]:
    """Recursively traverses a pipeline's structure and collects all encountered tasks. A root task can be handed over
    on its own, since a single task is never an iterable of them.

    Args:
        to_traverse (Task | Iterable[Task]): A pipeline given by its root task, or the tasks of one.

    Yields:
        Task: The encountered tasks, each one followed by everything it requires.
    """
    for task in [to_traverse] if isinstance(to_traverse, Task) else to_traverse:
        yield task
        yield from traverse_pipeline(task.requires().values())
