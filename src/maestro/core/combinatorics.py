"""Contains the task and parameter classes used to model pipelines, and the combinators they produce."""

from __future__ import annotations

from abc import ABCMeta
from functools import cache, partial
from typing import TYPE_CHECKING, Any

from maestro.core.synthesis import Constructor, SpecificationBuilder

if TYPE_CHECKING:
    from collections.abc import Callable, KeysView, Mapping, Sequence

    from maestro.core.execution import Target
    from maestro.core.synthesis import Specification


class TaskParameter:
    """Declares that a Task requires another one.

    Attributes:
        required_task (type[Task]): The type of Task this parameter wraps.
        unique_across_prior_tasks (bool): Whether to enforce that all concrete occurrences of the potentially abstract wrapped type need to be the same.
    """

    def __init__(self, required_task: type[Task], *, unique_across_prior_tasks: bool = False):
        """Initializes the TaskParameter. Setting unique_across_prior_tasks to True only makes sense if the
        required_task is abstract.

        Args:
            required_task (type[Task]): The type of Task this parameter wraps.
            unique_across_prior_tasks (bool): Whether to enforce that all concrete occurrences of the potentially abstract wrapped type need to be the same.
        """
        self.required_task = required_task
        self.unique_across_prior_tasks = unique_across_prior_tasks

    def __get__(self, instance: Task | None, owner: type[Task] | None = None) -> Any:
        """Reading this off an instance yields the task, since binding shadows this declaration. Reading it off the
        class yields this.

        Returns:
            Any: This declaration or in practice the instance of the task that replaced it.
        """
        return self


class Task(metaclass=ABCMeta):
    """A Task is one step in a pipeline. This class exists to automatically create typed combinators for synthesis.

    Attributes:
        dfs (list[tuple[str, bool]]): The pipeline contents, post-order DFS. Tasks that had variance marked with true.
        task_id (str): Identity between tasks.
    """

    dfs: list[tuple[str, bool]]
    task_id: str

    def __init__(self, *requirements: Task):
        """One of the few instance level methods. Zips each argument to the concrete task it got during synthesis.

        Args:
            *requirements (Task): The tasks this task depends on.
        """
        for (name, _), required in zip(self._requirements().items(), requirements, strict=True):
            setattr(self, name, required)

        # Constructed by induction, every requirement has already set its own dfs
        self.dfs = [
            step
            for (_, parameter), required in zip(self._requirements().items(), requirements, strict=True)
            for step in (*required.dfs, (type(required).__name__, bool(parameter.required_task.get_all_variants())))
        ]
        self.task_id = self.identity()

    def output(self) -> Mapping[str, Target] | None:
        """Specifies what a task outputs via Targets. Other tasks in the pipeline request the contents of this Mapping
        to resume work "hot".

        Must be overridden by subclasses.

        Returns:
            Mapping[str, Target] | None: The produced outputs. None if the task has nothing to output.
        """
        return None

    def run(self) -> None:
        """The business logic of this task. This should produce the outputs."""

    def complete(self) -> bool:
        """Whether the work is already done and can be skipped. By default, this means that the task's outputs are all
        present.

        Returns:
            bool: Whether the task is complete.
        """
        outputs = self.output()
        if not outputs:
            return False
        return all(target.exists() for target in outputs.values())

    def identity(self) -> str:
        """Another instance level method. Sets task_id.

        Pipelines differing anywhere below are different tasks all the way up. It can be flat because a task's arity is
        fixed by its class, which means the sequence of names fully describes the shape.

        Returns:
            str: The identity of this task.
        """
        return "-".join([*(name for name, _ in self.dfs), type(self).__name__])

    @property
    def variant_label(self) -> str:
        """A variant label guaranteed to be safe for writing output files. This is just the task_id with pipeline steps
        that couldn't have variance removed (keeps filenames short with less clutter).

        Returns:
            str: The label.
        """
        return "-".join([*(name for name, varied in self.dfs if varied), type(self).__name__])

    def requires(self) -> dict[str, Task]:
        """The tasks required to be run before this one, by parameter names.

        Returns:
            dict[str, Task]: The required tasks.
        """
        return {name: getattr(self, name) for name in self._requirements()}

    def input(self) -> dict[str, Any]:
        """The outputs of the required tasks, also by parameter names.

        Returns:
            dict[str, Any]: What each required task produced.
        """
        return {name: task.output() for name, task in self.requires().items()}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Defining a new Task makes the variant cache stale."""
        super().__init_subclass__(**kwargs)
        Task.get_all_variants.cache_clear()

    @classmethod
    @cache
    def get_all_variants(cls) -> KeysView[type[Task] | Any]:
        """Recursively finds all subclasses of current class.

        Returns:
            KeysView[type[Task] | Any]: All subclasses, in definition order.
        """
        return dict.fromkeys(
            variant for child in cls.__subclasses__() for variant in (child, *child.get_all_variants())
        ).keys()

    @classmethod
    @cache
    def _requirements(cls) -> Mapping[str, TaskParameter]:
        """Collects all attributes of tasks along the MRO, then retrieves all TaskParameters from them. This allows
        TaskParameters to work with inheritance.

        Each entry results in one non-rightmost entry in the arrow type of the resulting combinator.

        Returns:
            Mapping[str, TaskParameter]: The accumulated TaskParameters.
        """
        return {
            name: value
            for name, value in {
                name: value
                for parent in reversed(cls.__mro__)
                if issubclass(parent, Task)
                for name, value in vars(parent).items()
            }.items()
            if isinstance(value, TaskParameter)
        }

    @classmethod
    @cache
    def requirements_unique_in_prior_tasks(cls) -> Mapping[str, TaskParameter]:
        """Filters the output of _requirements, returning only those dict entries where the TaskParameter has the
        optional unique_across_prior_tasks flag set.

        Returns:
            Mapping[str, TaskParameter]: The filtered output of _requirements.
        """
        return {
            k: task_parameter
            for k, task_parameter in cls._requirements().items()
            if task_parameter.unique_across_prior_tasks
        }

    @classmethod
    @cache
    def unique_required_tasks_in_prior(cls) -> Sequence[type[Task]]:
        """Transforms the output of requirements_unique_in_prior_tasks into a list of classes that the collected
        TaskParameter's indicate should be unique across prior tasks.

        Returns:
            Sequence[type[Task]]: The transformed output of requirements_unique_in_prior_tasks.
        """
        return [task_parameter.required_task for task_parameter in cls.requirements_unique_in_prior_tasks().values()]

    @classmethod
    @cache
    def target(cls) -> Constructor:
        """The target constructed by this class. This is the right-most entry of the resulting arrow-type constructed
        for a given Task.

        Returns:
            Constructor: A Constructor, uniquely identified by the class name.
        """
        return Constructor(cls.__name__)

    @classmethod
    def constraints(cls) -> Sequence[Callable[..., bool]]:
        """The Callables returned by this class are translated into constraints applied to the resulting combinator's
        types. This method is intended to be overridden in subclasses to make use of this feature. The returned
        Callables are directly passed to a SpecificationBuilder as a .constraint() call.

        Returns:
            Sequence[Callable[..., bool]]: A sequence of constraints.
        """
        return []

    @classmethod
    def __constraints(cls) -> Sequence[Callable[..., bool]]:
        """This method computes the auto-generated constraints that result from features that are part of the framework
        itself. For instance, the unique_across_prior_tasks flag is implemented by adding a constraint, this method
        creates the corresponding Callables.

        Returns:
            Sequence[Callable[..., bool]]: The auto-generated constraints.
        """
        from maestro.constraints.unique import is_unique_in_prior_tasks  # noqa: PLC0415

        if cls.requirements_unique_in_prior_tasks():
            return [partial(is_unique_in_prior_tasks, required_to_be_unique=cls.unique_required_tasks_in_prior())]
        return []

    @classmethod
    def combinator_type(cls) -> Specification:
        """Computes the resulting type of the combinator represented by this class, as described in the methods
        referenced by this method.

        Returns:
            Specification: The type of the combinator.
        """
        sp = SpecificationBuilder()
        for name in [v.required_task.__name__ for v in cls._requirements().values()]:
            sp = sp.argument(name, Constructor(name))
        for constraint in cls.__constraints():
            sp = sp.constraint(constraint)
        for constraint in cls.constraints():
            sp = sp.constraint(constraint)
        return sp.suffix(cls.target())

    @classmethod
    def combinator(cls) -> tuple[str, Callable[..., Task], Specification]:
        """Produces the typed combinator representing this class. Note that it is not necessary to use kwargs here, as
        the generation of the type guarantees that the order of passed args and TaskParameters aligns.

        Returns:
            tuple[str, Callable[..., Task], Specification]: The resulting combinator.
        """
        return cls.__name__, lambda *args: cls(*args), cls.combinator_type()
