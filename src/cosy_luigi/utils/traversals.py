"""_summary_."""

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
    """_summary_.

    Args:
        *heterogeneous_task_collection (type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]): _description_

    Returns:
         Iterable[type[CoSyLuigiTask]]: _description_
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
    """_summary_.

    Args:
        vs (Sequence[CoSyLuigiTask] | Iterable[CoSyLuigiTask]): _description_

    Returns:
        Sequence[CoSyLuigiTask]: _description_
    """
    result: list[CoSyLuigiTask] = [*vs]
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
