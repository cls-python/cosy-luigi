"""Contains the function for the constraint that makes a required task be unique throughout all prior tasks in a
pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from cosy_luigi.utils import traverse_pipeline

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cosy_luigi import CoSyLuigiTask


def _is_unique_in_prior_tasks(
    vs: Mapping[str, CoSyLuigiTask], required_to_be_unique: Sequence[type[CoSyLuigiTask]]
) -> bool:
    """Examines the output of traverse_pipeline against a Sequence of CoSyLuigiTask's types that are intended to be
    unique. Whenever a task that is a subclass or the required to be unique class itself is encountered in the
    pipeline, remember the encountered task. If any further task that is a subclass that is not identical to the
    previously encountered task is encountered, return False, else return True.

    Encountering two different subclasses within the same pipeline for given required to be unique tasks means that
    it is not unique.

    Args:
        vs (Mapping[str, CoSyLuigiTask]): The variables passed to the function by CoSy during synthesis. Populated by the partial pipelines beginning at current tasks required tasks.
        required_to_be_unique (Sequence[type[CoSyLuigiTask]]): The CoSyLuigiTasks' types that are intended to be unique.

    Returns:
        bool: True if all types contained in required_to_be_unique are unique, False otherwise.
    """
    classes = [pc.__class__ for pc in traverse_pipeline(vs.values())]
    seen_subclasses: dict[type[CoSyLuigiTask], type[CoSyLuigiTask]] = {}
    for c in classes:
        for unique in required_to_be_unique:
            if issubclass(c, unique):
                if unique in seen_subclasses:
                    if seen_subclasses[unique] != c:
                        return False
                else:
                    seen_subclasses[unique] = c
    return True


def is_unique_in_prior_tasks(
    vs: Mapping[str, CoSyLuigiTask], required_to_be_unique: type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]
) -> bool:
    """Wrapper around _is_unique_in_prior_tasks that allows passing either a single type of a CoSyLuigiTask or a
    Sequence of CoSyLuigiTasks' types.

    Args:
        vs (Mapping[str, CoSyLuigiTask]): The variables passed to the function by CoSy during synthesis. Populated by the partial pipelines beginning at current tasks required tasks.
        required_to_be_unique (type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]): The CoSyLuigiTask's type or types that are intended to be unique.

    Returns:
        bool: True if all types contained in required_to_be_unique are unique, False otherwise.
    """
    return _is_unique_in_prior_tasks(
        vs,
        required_to_be_unique if isinstance(required_to_be_unique, Sequence) else [required_to_be_unique],
    )
