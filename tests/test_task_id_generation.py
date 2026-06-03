"""Tests if the task_id generation for CoSyLuigiTasks is working as intended, preventing shading from happening.
Shading is defined to be a task that looks identical to another task, because it has the same required tasks and the
same name as another task, however tasks that are more than one step earlier in the pipeline still alter its outputs.
In this case, the task_id that CoSyLuigiTask sets must signal this to the Luigi scheduler, so that it knows that
these tasks are different pipelines. This can happen due to CoSy-Luigi operating on the logic that a task having an
output depending on prior tasks means introducing ad-hoc polymorphism.

In short, CoSy-Luigi considers tasks with identical names, identical requirements, and identical outputs to be
Singletons. This differs from Luigi, which considers tasks with identical names and identical requirements
Singletons. This means Luigi allows shading, and CoSy-Luigi does not."""

from abc import ABC

import luigi
import pytest
from cosy.maestro import Maestro
from luigi.mock import MockTarget
from luigi.task import task_id_str

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter

target_a = MockTarget("A")
target_b = MockTarget("B")
fs = target_a.fs


class Shaded(CoSyLuigiTask, ABC):
    """Abstract base class for shaded tasks."""

    identifier: str


class ShadedA(Shaded):
    """An example for a class that can be shaded."""

    identifier = "A"


class ShadedB(Shaded):
    """An example of a class that can be shaded."""

    identifier = "B"


class Shade(CoSyLuigiTask):
    """Shades either ShadedA or ShadedB. They are shaded because this task gives no indication of the contents of the
    shaded CoSyLuigiTaskParameter as part of the signature that classes requesting this task see, i.e. a Shade(
    ShadeA()) and Shade(ShadeB()) looks identical to Evaluate at Runtime."""

    shaded = CoSyLuigiTaskParameter(Shaded)

    def complete(self):
        """Marks this class as always complete (avoids needing an output for testing).

        Returns:
            bool: True
        """
        return True


class Evaluate(CoSyLuigiTask):
    """Behaves as either a Singleton or as different Pipelines depending on how task_ids are assigned. Correct
    behaviour is the latter."""

    shade = CoSyLuigiTaskParameter(Shade)

    def run(self):
        """Write "OK". into the task's output file."""
        with self.output()["output"].open("w") as f:
            f.write("OK.")

    def output(self):
        """This task creates a different output and thus is intended to act as a different task.

        Returns:
            Mapping[str, MockTarget]: An output with a different name depending on the shaded task.
        """
        return {"output": MockTarget(self.shade.shaded.identifier)}


class EvaluateWithPotentialToShade(Evaluate):
    """Behaves as a Singleton, showcases what happens when using the default task_id allocation of Luigi.

    Attributes:
        task_id (str): The task_id that Luigi would assign.
    """

    def __init__(self, *args, **kwargs):
        """Initializes the task and overrides the task_id with the default allocation from Luigi.

        Args:
            *args (_type_): Passed through to super().__init__().
            **kwargs (_type_): Passed through to super().__init__().
        """
        super().__init__(*args, **kwargs)
        # Copy the behaviour of regular Luigi
        self.task_id = task_id_str(self.get_task_family(), self.to_str_params(only_significant=True, only_public=True))
        self.__hash = hash(self.task_id)


@pytest.fixture
def repo():
    """Creates a CoSyLuigiRepo for testing.

    Returns:
        CoSyLuigiRepo: The CoSyLuigiRepo for testing.
    """
    return CoSyLuigiRepo(Evaluate, Shade, Shaded)


@pytest.fixture
def shadeable_repo():
    """Creates a CoSyLuigiRepo for testing that allows shading to happen.

    Returns:
        CoSyLuigiRepo: The CoSyLuigiRepo for testing.
    """
    return CoSyLuigiRepo(EvaluateWithPotentialToShade, Shade, Shaded)


def test_shading_not_possible(repo):
    """Tests that shading does not occur when using the task_id that CoSyLuigiTask computes.

    Args:
        repo (CoSyLuigiRepo): The CoSyLuigiRepo for testing.
    """
    fs.clear()
    assert not target_a.exists()
    assert not target_b.exists()
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    luigi.build(maestro.query(Evaluate.target()), local_scheduler=True, detailed_summary=True)
    assert target_a.exists()
    assert target_b.exists()


def test_shading_would_be_possible(shadeable_repo):
    """Tests that shading occurs when using the task_id default to Luigi.

    Args:
        repo (CoSyLuigiRepo): The CoSyLuigiRepo containing the overridden CoSyLuigiTask that allows for shading.
    """
    fs.clear()
    assert not target_a.exists()
    assert not target_b.exists()
    maestro = Maestro(
        shadeable_repo.cls_repo,
        shadeable_repo.taxonomy,
    )
    luigi.build(maestro.query(EvaluateWithPotentialToShade.target()), local_scheduler=True, detailed_summary=True)
    assert not (target_a.exists() and target_b.exists())


def test_output_mapping_is_enforced():
    """Computing the task_ids for CoSyLuigiTasks assumes that the outputs are Mapping[str, Target]. Tests that this
    enforced."""

    class TaskWithWrongOutputA(CoSyLuigiTask):
        """Class with a wrong output mapping."""

        def output(self):
            """Does not return a Mapping but a raw Target object.

            Returns:
                MockTarget: The Target object.
            """
            return MockTarget("")

    with pytest.raises(TypeError):
        TaskWithWrongOutputA()

    class TaskWithWrongOutputB(CoSyLuigiTask):
        """Class with a wrong output mapping."""

        def output(self):
            """Does not return a Mapping but a Target object wrapped in a list.

            Returns:
                list[MockTarget]: A list of Target objects.
            """
            return [MockTarget("")]

    with pytest.raises(TypeError):
        TaskWithWrongOutputB()

    class TaskWithNoneOutput(CoSyLuigiTask):
        """A class with no outputs."""

    # This is explicitly allowed, as having no output means this class is never ad-hoc polymorphic.
    TaskWithNoneOutput()
