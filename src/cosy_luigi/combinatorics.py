from __future__ import annotations

from collections import defaultdict
from functools import cache
from typing import TYPE_CHECKING

import luigi
from cosy.core import Constructor, SpecificationBuilder
from luigi.task_register import Register

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from cosy.core.synthesizer import Specification


class CoSyLuigiTaskParameter(luigi.TaskParameter):
    def __init__(self, required_task: type[CoSyLuigiTask]):
        super().__init__()
        self.required_task = required_task


class CoSyLuigiTask(luigi.Task):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.self = cls

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
    def _requirements(cls):
        return {
            k: v
            for k, v in cls.get_all_class_attributes().items()
            if not k.startswith("__") and not callable(v) and issubclass(v.__class__, CoSyLuigiTaskParameter)
        }

    @classmethod
    @cache
    def target(cls):
        return Constructor(cls.__name__)

    @classmethod
    def constraints(cls) -> Sequence[Callable[..., bool]]:
        return []

    @classmethod
    def combinator_type(cls):
        sp = SpecificationBuilder()
        for name in [v.required_task.__name__ for v in cls._requirements().values()]:
            sp = sp.argument(name, Constructor(name))
        for constraint in cls.constraints():
            sp = sp.constraint(constraint)
        return sp.suffix(cls.target())

    @classmethod
    def combinator(cls):
        if len(cls._requirements()) == 0:
            return cls.__name__, lambda: cls(), cls.combinator_type()
        return cls.__name__, lambda *args: cls(*args), cls.combinator_type()


class CoSyLuigiRepo:
    def __init__(self, *tasks: type[CoSyLuigiTask]):
        Register.disable_instance_cache()
        self.luigi_repo: list[type[CoSyLuigiTask]] = [*tasks]
        self.taxonomy: Mapping[str, set[str]] = defaultdict(set)
        self.cls_repo: list[tuple[str, Callable, Specification]] = []
        for task in self.luigi_repo:
            self.cls_repo.append(task.combinator())
            for tpe in task.mro()[1:]:
                if issubclass(tpe, CoSyLuigiTask):
                    # Is a subclass of CosyLuigiTask, but a superclass of task
                    self.taxonomy[task.__name__].add(tpe.__name__)
