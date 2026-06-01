"""Contains the classes that provide the core functionality of modeling pipelines with CoSy-Luigi."""

from __future__ import annotations

import logging
import textwrap
from collections import defaultdict
from collections.abc import Mapping
from functools import cache, partial
from typing import TYPE_CHECKING, Any

import luigi
from cosy.core import Constructor, SpecificationBuilder

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from cosy.core.synthesizer import Specification

logger = logging.getLogger(__name__)


class CoSyLuigiTaskParameter(luigi.TaskParameter):
    """Serves as CoSy-specific version of luigi.TaskParameter. It has two primary uses: Providing a unique classname
    for the reflection-based operations of the CoSyLuigiTask to examine, and wrapping the classes that a
    CoSyLuigiTask requires to be inhabited. For instance, for a class B, which declares a CoSyLuigiTaskParameter that
    wraps the Class A, the resulting CoSy combinator type would be A -> B.


    Attributes:
        required_task (type[CoSyLuigiTask]): The type of CoSyLuigiTask this parameter wraps.
        unique_across_prior_tasks (bool): Whether or not to enforce that all concrete occurences of the potentially abstract wrapped type need to be the same.
    """

    def __init__(self, required_task: type[CoSyLuigiTask], *, unique_across_prior_tasks: bool = False):
        """Initializes the CoSyLuigiTaskParameter. Setting unique_across_prior_tasks to True only makes sense if the
        required_task is abstract.

        Args:
            required_task (type[CoSyLuigiTask]): The type of CoSyLuigiTask this parameter wraps.
            unique_across_prior_tasks (bool): Whether or not to enforce that all concrete occurences of the potentially abstract wrapped type need to be the same.
        """
        super().__init__()
        self.required_task = required_task
        self.unique_across_prior_tasks = unique_across_prior_tasks


class CoSyLuigiTask(luigi.Task):
    """Serves as a CoSy-specific version of luigi.Task. Types derived from CoSyLuigiTask can be added to a
    CoSyLuigiRepo and will automatically produce typed combinators for synthesis. The main purpose of CoSyLuigiTask
    is to enforce some conventions and utilize reflections to model variance through Python object inheritance. A
    task that requires another task of a given type T will also find all subclasses of T as valid inputs."""

    def __init__(self, *args, **kwargs):
        """Initializes the CoSyLuigiTask. This only happens during and after synthesis, when the combinators are
        interpreted. Most functionality is instead implemented on class-level to benefit from caching,
        since constraints/predicates during synthesis can cause large numbers of instances to be created.

        Args:
            *args (_type_): Passed to super().__init__().
            **kwargs (_type_): Passed to super().__init__().

        Raises:
            TypeError: The output method of a CoSyLuigiTask must return a Mapping. This is done to prevent addressing output files by index, which leads to unreadable code.
        """
        super().__init__(*args, **kwargs)
        output = self.output()
        if not output:
            return
        if not isinstance(output, Mapping):
            msg = f"{self.__class__.__name__}'s output method does not return a Mapping. Unlike regular LuigiTasks, CoSyLuigiTasks must return None or a Mapping, i.e. a dict."
            raise TypeError(msg)
        # Map to filenames, str method of FileSystemTargets is path
        # We do not check for value type, as Luigi will throw Exception if its not a FileSystemTarget already
        self.task_id += "_" + "_".join(map(str, output.values()))
        self.__hash = hash(self.task_id)

    @classmethod
    @cache
    def get_all_variants(cls) -> set[type[CoSyLuigiTask] | Any]:
        """Recursively finds all subclasses of current class.

        Returns:
            set[type[CoSyLuigiTask] | Any]: The set of all subclasses.
        """
        return set(cls.__subclasses__()).union([s for c in cls.__subclasses__() for s in c.get_all_variants()])

    @classmethod
    @cache
    def get_all_class_attributes(cls) -> dict[str, Any]:
        """Collects all class attributes that will be present at runtime. Python does not store class attributes from
        parent classes in the dict of the subclasses. This method traverses the __mro__, the ordered list of
        superclasses to consider for looking for methods, up to the first class that no longer relates to CoSy and
        copies all their attributes down to the current class.

        Returns:
            dict[str, Any]: A dict containing all attributes that will be present at runtime.
        """
        attrs: dict[str, Any] = {}
        for c in [cc for cc in reversed(cls.__mro__) if issubclass(cc, CoSyLuigiTask)]:
            attrs.update(getattr(c, "__dict__", {}))
        return attrs

    def get_all_instance_attributes(self) -> dict[str, Any]:
        """Collects all attributes present on an instance at runtime. This method specifically accounts for the fact
        that Luigi switches out LuigiTaskParameters for the true passed objects at instantiation.

        Returns:
             dict[str, Any]: A dict containing all attributes that are present at runtime.
        """
        return {attr: getattr(self, attr) for attr in dir(self)}

    def requires(self) -> dict[str, CoSyLuigiTask]:
        """Returns a dict of other tasks required to run this task. This is done by retrieving all user-created
        attributes that are subclasses of CosyLuigiTaskParameter. Note that at Runtime Luigi unpacks
        CosyLuigiTaskParameters, so the actual check has to be for CoSyLuigiTasks. This overrides the requires method
        of luigi.Task, and thus ensures that the Luigi scheduler when executing a task "sees" the requirements
        allocated during synthesis.

        Returns:
            dict[str, CoSyLuigiTask]: A dict of other tasks required to run this task, keyed by attribute name.
        """
        return {
            k: v
            for k, v in self.get_all_instance_attributes().items()
            if not k.startswith("__") and not callable(v) and issubclass(v.__class__, CoSyLuigiTask)
        }

    @classmethod
    @cache
    def _requirements(cls) -> Mapping[str, CoSyLuigiTaskParameter]:
        """Filters all class attributes present at runtime to only contain CoSyLuigiTaskParameters. Its primary use
        is collecting the potentially abstract requirements so that a type for the combinator can be generated. Each
        entry in the dict returned by this method results in one non-rightmost entry in the arrow type of the
        resulting combinator.

        Returns:
            Mapping[str, CoSyLuigiTaskParameter]: The CoSyLuigiTaskParameters present on the class, keyed by attribute name.
        """
        return {
            k: v
            for k, v in cls.get_all_class_attributes().items()
            if not k.startswith("__") and not callable(v) and issubclass(v.__class__, CoSyLuigiTaskParameter)
        }

    @classmethod
    @cache
    def get_params(cls) -> list[tuple[str, CoSyLuigiTaskParameter]]:
        """Converts the output of _requirements into a list of tuples instead of a dict.

        Returns:
            list[tuple[str, CoSyLuigiTaskParameter]]: A list of tuples representing the output of _requirements.
        """
        return list(cls._requirements().items())

    @classmethod
    @cache
    def requirements_unique_in_prior_tasks(cls) -> Mapping[str, CoSyLuigiTaskParameter]:
        """Filters the output of _requirements, returning only those dict entries where the CoSyLuigiTaskParameter
        has the optional unique_across_prior_tasks flag set.

        Returns:
            Mapping[str, CoSyLuigiTaskParameter]: The filtered output of _requirements.
        """
        return {
            k: task_parameter
            for k, task_parameter in cls._requirements().items()
            if task_parameter.unique_across_prior_tasks
        }

    @classmethod
    @cache
    def unique_required_tasks_in_prior(cls) -> Sequence[type[CoSyLuigiTask]]:
        """Transforms the output of requirements_unique_in_prior_tasks into a list of classes that the collected
        CoSyLuigiTaskParameter's indicate should be unique across prior tasks.

        Returns:
            Sequence[type[CoSyLuigiTask]]: The transformed output of requirements_unique_in_prior_tasks.
        """
        return [task_parameter.required_task for task_parameter in cls.requirements_unique_in_prior_tasks().values()]

    @classmethod
    @cache
    def target(cls) -> Constructor:
        """The target constructed by this class. This is the right-most entry of the resulting arrow-type constructed
        for a given CoSyLuigiTask.

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
        """This method computes the auto-generated constraints that results from features that are part of the
        framework itself. For instance, the unique_across_prior_tasks flag is implemented by adding a constraint,
        this method creates the corresponding Callables.

        Returns:
            Sequence[Callable[..., bool]]: The auto-generated constraints.
        """
        from cosy_luigi.constraints.unique import _is_unique_in_prior_tasks  # noqa: PLC0415

        if cls.requirements_unique_in_prior_tasks():
            return [partial(_is_unique_in_prior_tasks, required_to_be_unique=cls.unique_required_tasks_in_prior())]
        return []

    @classmethod
    def combinator_type(cls) -> Specification:
        """Computes the resulting type of the combinator represented by this class, as described in the methods referenced by this method.

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
    def combinator(cls) -> tuple[str, Callable[..., CoSyLuigiTask], Specification]:
        """Produces the typed combinator representing this class. Classes with no requirements are instantiated as
        is, while classes with requirements have the resulting values for their task-parameters passed as varargs.
        Note that it is not necessary to use kwargs here, as the generation of the type guarantees that the order of
        passed args and CoSyLuigiTaskParameters aligns.

        Returns:
            tuple[str, Callable[..., CoSyLuigiTask], Specification]: The resulting combinator.
        """
        if len(cls._requirements()) == 0:
            return cls.__name__, lambda: cls(), cls.combinator_type()
        return cls.__name__, lambda *args: cls(*args), cls.combinator_type()


class CoSyLuigiRepo:
    """Serves as the repository that Combinatory Logic Synthesis requires for type inhabitation. Unlike the previous
    version of CoSy-Luigi, known as CLS-Luigi, having an explicit repository prevents side-effects that may occur by
    collecting all CoSyLuigiTasks via reflection. Another important task of the CoSyLuigiRepo is to translate the
    observed class hierarchy into a taxonomy that the CoSy framework understands during synthesis.

    Attributes:
        luigi_repo (set[type[CoSyLuigiTask]]): The set of CoSyLuigiTask types that constitute the repositories' combinators.
        taxonomy (Mapping[str, set[str]]): The taxonomy that describes the class hierarchy of the luigi_repo.
        cls_repo (list[tuple[str, Callable, Specification]]): The final repository that can be passed to the CoSy framework.
    """

    def __init__(self, *tasks: type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]):
        # Accepts completely heterogeneous nested collections

        # This doesn't technically need to unpack as flatten could be typed to accept packed tuples
        # But performance is equivalent/faster because the first layer doesn't need to be checked this way
        """Initializes the CoSyLuigiRepo. The passed arbitrarily nested Sequence is flattened and converted into a
        set. Please see the documentation of flatten, as it adds some features to the flattening. The taxonomy is
        then computed by examining the method resolution order of each CoSyLuigiTask type, up to the most abstract
        possible CoSyLuigiTask itself.

        Also performs rudimentary inspection of the repositories' contents, currently only checks if any set
        unique_in_prior_tasks flags make logical sense.

        Args:
            *tasks (type[CoSyLuigiTask] | Sequence[type[CoSyLuigiTask]]): An arbitrarily nested Sequence where leaves are CoSyLuigiTasks' types.
        """
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
        """Checks if the unique_in_prior_tasks flags set on CoSyLuigiTaskParameters make logical sense. If a required
        task is set to be unique throughout pipelines, but there are no subclasses of it present, i.e. no variance is
        possible, remind the user that this is nonsensical.
        """
        for source_task, param_name, required_type in [
            (task, _, required_unique_task.required_task)
            for task in self.luigi_repo
            for _, required_unique_task in task.requirements_unique_in_prior_tasks().items()
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
