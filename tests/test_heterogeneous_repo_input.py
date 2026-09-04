"""Tests if flatten correctly flattens arbitrarily nested sequences."""

from maestro import Repository, Task
from maestro.utils import flatten


class TaskA(Task):
    """Placeholder Task A."""


class TaskB(Task):
    """Placeholder Task B."""


class TaskC(Task):
    """Placeholder Task C."""


class TaskD(Task):
    """Placeholder Task D."""


class TaskE(Task):
    """Placeholder Task E."""


class TaskF(Task):
    """Placeholder Task F."""


class TaskG(Task):
    """Placeholder Task G."""


class TaskH(Task):
    """Placeholder Task H."""


class TaskI(Task):
    """Placeholder Task I."""


class TaskJ(Task):
    """Placeholder Task J."""


class TaskK(Task):
    """Placeholder Task K."""


def test_heterogeneous_repo_input():
    """Test if instantiating a Repository for a 2-times nested mixed Sequence of Sequences leads to the task_repo
    containing a set of the types contained in the Sequence of  Tasks. The Sequence is passed through the varargs of the
    Repository constructor."""
    repo = Repository(TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK]))
    assert repo.task_repo == {TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK}


def test_heterogeneous_args_input():
    """Test if calling flatten on a 2-times nested mixed Sequence of Sequences returns a set containing the types
    contained in the Sequence of Tasks. The Sequence is passed through the varargs of flatten."""
    flattened_collection = set(
        flatten(TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK]))
    )
    assert flattened_collection == {TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK}


def test_heterogeneous_list_input():
    """Test if calling flatten on a 2-times nested mixed list of Sequences returns a set containing the types contained
    in the list of Tasks. The list is passed as a single argument to flatten."""
    flattened_collection = list(
        flatten([TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK])])
    )
    assert flattened_collection == [TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK]


def test_heterogeneous_tuple_input():
    """Test if calling flatten on a 2-times nested mixed tuple of Sequences returns a set containing the types contained
    in the tuple of Tasks. The tuple is passed as a single argument to flatten."""
    flattened_collection = tuple(
        flatten((TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK])))
    )
    assert flattened_collection == (TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK)
