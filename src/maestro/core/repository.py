"""Contains the repository that holds the tasks the synthesis may use."""

from __future__ import annotations

import logging
import textwrap
from collections import defaultdict
from typing import TYPE_CHECKING

from maestro.core.combinatorics import Task

if TYPE_CHECKING:
    from collections.abc import Callable, KeysView, Mapping, Sequence

    from maestro.core.synthesis import Specification

logger = logging.getLogger(__name__)


class Repository:
    """Serves as the repository to construct a taxonomy and tree-grammar from.

    Attributes:
        task_repo (KeysView[type[Task]]): The Task types that constitute the repositories' combinators.
        taxonomy (Mapping[str, set[str]]): The taxonomy that describes the class hierarchy of the task_repo.
        cls_repo (list[tuple[str, Callable, Specification]]): The final repository that can be passed to the Maestro.
    """

    def __init__(self, *tasks: type[Task] | Sequence[type[Task]]):
        # Accepts completely heterogeneous nested collections

        # This doesn't technically need to unpack as flatten could be typed to accept packed tuples
        # But performance is equivalent/faster because the first layer doesn't need to be checked this way
        """Initializes the Repository. The passed arbitrarily nested Sequence is flattened and deduplicated. Please
        see the documentation of flatten, as it adds some features to the flattening. The taxonomy is then computed
        by examining the method resolution order of each Task type, up to the most abstract possible Task itself.

        Also performs rudimentary inspection of the repositories' contents, currently only checks if any set
        unique_in_prior_tasks flags make logical sense.

        Args:
            *tasks (type[Task] | Sequence[type[Task]]): An arbitrarily nested Sequence where leaves are Tasks' types.
        """
        from maestro.utils import flatten  # noqa: PLC0415

        # Deduplicated but with order preserved
        self.task_repo: KeysView[type[Task]] = dict.fromkeys(flatten(*tasks)).keys()
        self.check_unique_in_prior_tasks_sanity()
        taxonomy: dict[str, set[str]] = defaultdict(set)
        self.cls_repo: list[tuple[str, Callable, Specification]] = []
        for task in self.task_repo:
            self.cls_repo.append(task.combinator())
            for supertype in task.mro()[1:]:
                # Every Task above this one, but no ABCs and mixins
                if issubclass(supertype, Task):
                    taxonomy[task.__name__].add(supertype.__name__)
        self.taxonomy: Mapping[str, set[str]] = dict(taxonomy)

    def check_unique_in_prior_tasks_sanity(self):
        """Checks if the unique_in_prior_tasks flags set on TaskParameters make logical sense. If a required task is set
        to be unique throughout pipelines, but there are no subclasses of it present, i.e. no variance is possible,
        remind the user that this is nonsensical.
        """
        for source_task, param_name, required_type in [
            (task, _, required_unique_task.required_task)
            for task in self.task_repo
            for _, required_unique_task in task.requirements_unique_in_prior_tasks().items()
            if not any(
                issubclass(task, required_unique_task.required_task) and task is not required_unique_task.required_task
                for task in self.task_repo
            )
        ]:
            logger.warning(
                textwrap.dedent(
                    f"""
                        =================================================================
                            WARNING ABOUT POTENTIALLY INCORRECT MODEL

                            Class:      {source_task.__name__}
                            Parameter:  {param_name}
                            Type:       {required_type.__name__}

                                        is required to be unique, but there are no sub-
                                        classes of it present in the repository. Either
                                        you forgot adding sub-classes of it to the rep-
                                        ository or this will not behave as expected.

                                        Please head over to the documentation:
                        =================================================================
                    """
                )
            )
