from __future__ import annotations

from typing import TYPE_CHECKING

from cosy_luigi import CoSyLuigiTask
from cosy_luigi.utils import traverse_pipeline

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


def _is_unique_in_prior_tasks(
    vs: Mapping[str, CoSyLuigiTask], required_to_be_unique: Iterable[type[CoSyLuigiTask]]
) -> bool:
    classes = [pc.__class__ for pc in traverse_pipeline(vs.values())]
    seen_subclasses = {}
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
    vs: Mapping[str, CoSyLuigiTask], required_to_be_unique: type[CoSyLuigiTask] | Iterable[type[CoSyLuigiTask]]
) -> bool:
    return _is_unique_in_prior_tasks(
        vs, [required_to_be_unique] if isinstance(required_to_be_unique, CoSyLuigiTask) else required_to_be_unique
    )
