from __future__ import annotations

import inspect
from abc import ABC
from collections import defaultdict
from functools import cache
from typing import TYPE_CHECKING, cast, TypeVarTuple

import luigi
from cosy.core import Constructor, SpecificationBuilder
from luigi.task_register import Register

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

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
    def __init__(self, *tasks: *tuple[type[CoSyLuigiTask] | Iterable[type[CoSyLuigiTask]], ...]):
        Register.disable_instance_cache()

        # Accepts completely heterogeneous nested collections
        def flatten(*heterogeneous_task_collection: *tuple[type[CoSyLuigiTask] | Iterable[type[CoSyLuigiTask]], ...]):
            return (
                task
                for task_or_task_collection in heterogeneous_task_collection
                for task in (
                    flatten(*cast(Iterable[type[CoSyLuigiTask]],task_or_task_collection))
                    if isinstance(task_or_task_collection, (tuple, list))
                    else cast(type[CoSyLuigiTask],task_or_task_collection).get_all_variants()
                    if inspect.isabstract(task_or_task_collection) or ABC in cast(type[CoSyLuigiTask],task_or_task_collection).__bases__
                    else (task_or_task_collection,)
                )
            )
        # This doesn't technically need to unpack as flatten could be typed to accept packed tuples
        # But performance is equivalent/faster because the first layer doesn't need to be checked this way
        self.luigi_repo: set[type[CoSyLuigiTask]] = set(flatten(*tasks))
        self.taxonomy: Mapping[str, set[str]] = defaultdict(set)
        self.cls_repo: set[tuple[str, Callable, Specification]] = set()
        for task in self.luigi_repo:
            self.cls_repo.add(task.combinator())
            for tpe in task.mro()[1:]:
                if issubclass(tpe, CoSyLuigiTask):
                    # Is a subclass of CosyLuigiTask, but a superclass of task
                    self.taxonomy[task.__name__].add(tpe.__name__)
