"""_summary_."""
from __future__ import annotations

import inspect
from abc import ABC
from typing import TYPE_CHECKING, cast, Any, Generator

from cosy_luigi import CoSyLuigiTask

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


def flatten(*heterogeneous_task_collection: type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]) ->  Generator[type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]] | Any, Any, None]:
    """_summary_.

    Args:
        *heterogeneous_task_collection (type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]): _description_

    Returns:
         Generator[type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]] | Any, Any, None]: _description_
    """
    return (
        task
        for task_or_task_collection in heterogeneous_task_collection
        for task in (
            flatten(*cast("Sequence[type[CoSyLuigiTask]]", task_or_task_collection))
            if isinstance(task_or_task_collection, (tuple, list))
            else cast("type[CoSyLuigiTask]", task_or_task_collection).get_all_variants()
            if inspect.isabstract(task_or_task_collection)
            or ABC in cast("type[CoSyLuigiTask]", task_or_task_collection).__bases__
            else (task_or_task_collection,)
        )
    )


def _traverse_pipeline(vs: Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask]) -> Sequence[CoSyLuigiTask]:
    """_summary_.

    Args:
        vs (Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask]): _description_

    Returns:
        Sequence[CoSyLuigiTask]: _description_
    """
    result = [*vs]
    for v in vs:
        result.extend(traverse_pipeline(v.requires().values()))
    return result


def traverse_pipeline(
    to_traverse: CoSyLuigiTask | Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask],
) -> Sequence[CoSyLuigiTask]:
    """_summary_.

    Args:
        to_traverse (CoSyLuigiTask | Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask]): _description_

    Returns:
        Sequence[CoSyLuigiTask]: _description_
    """
    return (
        _traverse_pipeline([to_traverse]) if isinstance(to_traverse, CoSyLuigiTask) else _traverse_pipeline(to_traverse)
    )
