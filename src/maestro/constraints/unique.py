"""Contains the function for the constraint that makes a required task be unique throughout all prior tasks in a
pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from maestro.utils import traverse_pipeline

if TYPE_CHECKING:
    from collections.abc import Mapping

    from maestro import Task


def is_unique_in_prior_tasks(vs: Mapping[str, Task], required_to_be_unique: type[Task] | Sequence[type[Task]]) -> bool:
    """Examines the output of traverse_pipeline against the Task types that are intended to be unique. A type is unique
    when every task in the pipeline that is a subclass of it, or that type itself, is the very same class. Encountering
    two different subclasses of one required to be unique type means it is apparently not unique.

    Args:
        vs (Mapping[str, Task]): The variables passed to the function by the Maestro during synthesis. Populated by the partial pipelines beginning at current tasks required tasks.
        required_to_be_unique (type[Task] | Sequence[type[Task]]): The Task's type or types that are intended to be unique.

    Returns:
        bool: True if all types contained in required_to_be_unique are unique, False otherwise.
    """
    uniques = required_to_be_unique if isinstance(required_to_be_unique, Sequence) else (required_to_be_unique,)
    classes = {type(task) for task in traverse_pipeline(vs.values())}
    return all(len({c for c in classes if issubclass(c, unique)}) <= 1 for unique in uniques)
