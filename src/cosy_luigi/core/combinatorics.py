from __future__ import annotations

import logging
import textwrap
from collections import defaultdict
from functools import cache, partial
from typing import TYPE_CHECKING

import luigi
from cosy.core import Constructor, SpecificationBuilder
from luigi.task_register import Register

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from cosy.core.synthesizer import Specification

logger = logging.getLogger(__name__)


class CoSyLuigiTaskParameter(luigi.TaskParameter):
    def __init__(self, required_task: type[CoSyLuigiTask], *, unique_across_prior_tasks: bool = False):
        super().__init__()
        self.required_task = required_task
        self.unique_across_prior_tasks = unique_across_prior_tasks


class CoSyLuigiTask(luigi.Task):
    @classmethod
    @cache
    def get_all_variants(cls):
        return set(cls.__subclasses__()).union([s for c in cls.__subclasses__() for s in c.get_all_variants()])

    @classmethod
    @cache
    def get_all_class_attributes(cls):
        attrs = {}
        for c in [cc for cc in reversed(cls.__mro__) if issubclass(cc, CoSyLuigiTask)]:
            attrs.update(getattr(c, "__dict__", {}))
        return attrs

    def get_all_instance_attributes(self):
        return {attr: getattr(self, attr) for attr in dir(self)}

    def requires(self):
        """
        Returns a list of other tasks required to run this task.
        This is done by retrieving all user-created attributes that are subclasses of CosyLuigiTaskParameter.

        Note that at Runtime Luigi unpacks CosyLuigiTaskParameters, so the actual check has to be for CoSyLuigiTasks.

        :return: A list of other tasks required to run this task
        """
        return {
            k: v
            for k, v in self.get_all_instance_attributes().items()
            if not k.startswith("__") and not callable(v) and issubclass(v.__class__, CoSyLuigiTask)
        }

    @classmethod
    @cache
    def _requirements(cls) -> Mapping[str, CoSyLuigiTaskParameter]:
        return {
            k: v
            for k, v in cls.get_all_class_attributes().items()
            if not k.startswith("__") and not callable(v) and issubclass(v.__class__, CoSyLuigiTaskParameter)
        }

    @classmethod
    @cache
    def get_params(cls):
        return list(cls._requirements().items())

    @classmethod
    @cache
    def requirements_unique_in_prior_tasks(cls) -> Mapping[str, CoSyLuigiTaskParameter]:
        return {
            k: task_parameter
            for k, task_parameter in cls._requirements().items()
            if task_parameter.unique_across_prior_tasks
        }

    @classmethod
    @cache
    def unique_required_tasks_in_prior(cls) -> Sequence[type[CoSyLuigiTask]]:
        return [task_parameter.required_task for task_parameter in cls.requirements_unique_in_prior_tasks().values()]

    @classmethod
    @cache
    def target(cls):
        return Constructor(cls.__name__)

    @classmethod
    def constraints(cls) -> Sequence[Callable[..., bool]]:
        return []

    @classmethod
    def __constraints(cls) -> Sequence[Callable[..., bool]]:
        from cosy_luigi.constraints.unique import _is_unique_in_prior_tasks  # noqa: PLC0415

        if cls.requirements_unique_in_prior_tasks():
            return [partial(_is_unique_in_prior_tasks, required_to_be_unique=cls.unique_required_tasks_in_prior())]
        return []

    @classmethod
    def combinator_type(cls):
        sp = SpecificationBuilder()
        for name in [v.required_task.__name__ for v in cls._requirements().values()]:
            sp = sp.argument(name, Constructor(name))
        for constraint in cls.__constraints():
            sp = sp.constraint(constraint)
        for constraint in cls.constraints():
            sp = sp.constraint(constraint)
        return sp.suffix(cls.target())

    @classmethod
    def combinator(cls):
        if len(cls._requirements()) == 0:
            return cls.__name__, lambda: cls(), cls.combinator_type()
        return cls.__name__, lambda *args: cls(*args), cls.combinator_type()


class CoSyLuigiRepo:
    def __init__(self, *tasks: type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]):
        Register.disable_instance_cache()

        # Accepts completely heterogeneous nested collections

        # This doesn't technically need to unpack as flatten could be typed to accept packed tuples
        # But performance is equivalent/faster because the first layer doesn't need to be checked this way
        from cosy_luigi.utils import flatten  # noqa: PLC0415

        self.luigi_repo: set[type[CoSyLuigiTask]] = set(flatten(*tasks))
        self.check_unique_in_prior_tasks_sanity()
        self.taxonomy: Mapping[str, set[str]] = defaultdict(set)
        self.cls_repo: list[tuple[str, Callable, Specification]] = []
        for task in self.luigi_repo:
            self.cls_repo.append(task.combinator())
            for tpe in task.mro()[1:]:
                if issubclass(tpe, CoSyLuigiTask):
                    # Is a subclass of CosyLuigiTask, but a superclass of task
                    self.taxonomy[task.__name__].add(tpe.__name__)

    def check_unique_in_prior_tasks_sanity(self):
        for source_task, param_name, required_type in [
            (task, k, required_unique_task.required_task)
            for task in self.luigi_repo
            for k, required_unique_task in task.requirements_unique_in_prior_tasks().items()
            if not any(
                issubclass(task, required_unique_task.required_task) and task is not required_unique_task.required_task
                for task in self.luigi_repo
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
