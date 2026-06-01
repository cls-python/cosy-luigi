"""Contains helper methods centered around traversing collections or pipelines in ways specific to CoSy-Luigi."""

from __future__ import annotations

import inspect
from abc import ABC
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cosy_luigi import CoSyLuigiTask

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


def flatten(
    *heterogeneous_task_collection: type[CoSyLuigiTask] | Iterable[type[CoSyLuigiTask]],
) -> Iterable[type[CoSyLuigiTask]]:
    """Takes an arbitrarily nested Sequence where the leaves of the nested structure are CoSyLuigiTasks' types and
    flattens it. During flattening, if an abstract Task type is encountered, it is instead expanded into the set of
    its implementing subclasses and becomes part of the flattening procedure, i.e. also multiple levels of
    abstraction are correctly handled. See the corresponding test_abstract_variant_expansion.py for an example.

    Args:
        *heterogeneous_task_collection (type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]): An arbitrarily nested Sequence where leaves are CoSyLuigiTasks' types.

    Returns:
         Iterable[type[CoSyLuigiTask]]: The flattened representation of *heterogeneous_task_collection.
    """
    return (
        task  # type: ignore # guaranteed by recursion to be a type[CoSyLuigiTask] instead of an Iterable itself
        for task_or_task_collection in heterogeneous_task_collection
        for task in (
            flatten(*cast("Iterable[type[CoSyLuigiTask]]", task_or_task_collection))
            if isinstance(task_or_task_collection, (tuple, list))
            else cast("type[CoSyLuigiTask]", task_or_task_collection).get_all_variants()
            if inspect.isabstract(task_or_task_collection)
            or ABC in cast("type[CoSyLuigiTask]", task_or_task_collection).__bases__
            else (task_or_task_collection,)
        )
    )


def _traverse_pipeline(vs: Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask]) -> Sequence[CoSyLuigiTask]:
    """Recursively traverses a pipeline's structure and collect all encountered tasks.

    Args:
        vs (Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask]): The previously encountered tasks.

    Returns:
        Sequence[CoSyLuigiTask]: The encountered tasks.
    """
    result: list[CoSyLuigiTask] = [*vs]
    for v in vs:
        result.extend(_traverse_pipeline(v.requires().values()))
    return result


def traverse_pipeline(
    to_traverse: CoSyLuigiTask | Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask],
) -> Sequence[CoSyLuigiTask]:
    """Recursively traverses a pipeline's structure and collect all encountered tasks. Wraps _traverse_pipeline to
    allow direct root task of a Pipeline to be passed.

    Args:
        to_traverse (CoSyLuigiTask | Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask]): The pipeline to be traversed.

    Returns:
        Sequence[CoSyLuigiTask]: All tasks contained in the pipeline.
    """
    return (
        _traverse_pipeline([to_traverse]) if isinstance(to_traverse, CoSyLuigiTask) else _traverse_pipeline(to_traverse)
    )
