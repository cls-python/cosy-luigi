"""Tests if the identity assigned to Tasks is working as intended, preventing shading from happening. Shading is defined
to be a task that looks identical to another task, because it has the same required tasks and the same name as another
task, however tasks that are more than one step earlier in the pipeline still alter its outputs. In this case, the
identity that Task computes must signal this to the engine, so that it knows that these tasks are different pipelines.
This can happen due to the Maestro operating on the logic that a task having an output depending on prior tasks means
introducing ad-hoc polymorphism.

In short, the Maestro identifies a task by its own class together with the identities of everything it requires, so two
pipelines differing anywhere below are different tasks all the way up. Identifying tasks by class names alone, as the
naive rule further down does, is what allows shading."""

from abc import ABC

import pytest

from maestro import Maestro, MemoryTarget, Repository, Task, TaskParameter, run_pipelines

target_a = MemoryTarget("A")
target_b = MemoryTarget("B")


class Shaded(Task, ABC):
    """Abstract base class for shaded tasks."""

    identifier: str


class ShadedA(Shaded):
    """An example for a class that can be shaded."""

    identifier = "A"


class ShadedB(Shaded):
    """An example of a class that can be shaded."""

    identifier = "B"


class Shade(Task):
    """Shades either ShadedA or ShadedB. They are shaded because this task gives no indication of the contents of the
    shaded TaskParameter as part of the signature that classes requesting this task see, i.e. a Shade( ShadeA()) and
    Shade(ShadeB()) looks identical to Evaluate at Runtime."""

    shaded = TaskParameter(Shaded)

    def complete(self):
        """Marks this class as always complete (avoids needing an output for testing).

        Returns:
            bool: True
        """
        return True


class Evaluate(Task):
    """Behaves as either a Singleton or as different Pipelines depending on how tasks are identified. Correct behaviour
    is the latter."""

    shade = TaskParameter(Shade)

    def run(self):
        """Write "OK". into the task's output file."""
        with self.output()["output"].open("w") as f:
            f.write("OK.")

    def output(self):
        """This task creates a different output and thus is intended to act as a different task.

        Returns:
            Mapping[str, MemoryTarget]: An output with a different name depending on the shaded task.
        """
        return {"output": MemoryTarget(self.shade.shaded.identifier)}


class EvaluateWithPotentialToShade(Evaluate):
    """Behaves as a Singleton, showcasing what happens when tasks are identified by class names alone."""

    def identity(self):
        """Identifies the task by its own class and the classes of the tasks it requires, ignoring everything further
        down. Two pipelines differing only below their requirements then look like one task.

        Returns:
            str: The naive identity, as opposed to the structural one Task inherits.
        """
        return f"{type(self).__name__}({','.join(type(r).__name__ for r in self.requires().values())})"


@pytest.fixture
def repo():
    """Creates a Repository for testing.

    Returns:
        Repository: The Repository for testing.
    """
    return Repository(Evaluate, Shade, Shaded)


@pytest.fixture
def shadeable_repo():
    """Creates a Repository for testing that allows shading to happen.

    Returns:
        Repository: The Repository for testing.
    """
    return Repository(EvaluateWithPotentialToShade, Shade, Shaded)


def test_shading_not_possible(repo):
    """Tests that shading does not occur when using the identity that Task computes.

    Args:
        repo (Repository): The Repository for testing.
    """
    MemoryTarget.clear()
    assert not target_a.exists()
    assert not target_b.exists()
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    run_pipelines(maestro.query(Evaluate.target()), workers=1)
    assert target_a.exists()
    assert target_b.exists()


def test_shading_would_be_possible(shadeable_repo):
    """Tests that shading occurs when tasks are identified by class names alone.

    Args:
        shadeable_repo (Repository): The Repository containing the overridden Task that allows for shading.
    """
    MemoryTarget.clear()
    assert not target_a.exists()
    assert not target_b.exists()
    maestro = Maestro(
        shadeable_repo.cls_repo,
        shadeable_repo.taxonomy,
    )
    run_pipelines(maestro.query(EvaluateWithPotentialToShade.target()), workers=1)
    assert not (target_a.exists() and target_b.exists())
