"""Tests if flatten correctly flattens arbitrarily nested sequences."""

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask
from cosy_luigi.utils import flatten


class TaskA(CoSyLuigiTask):
    """Placeholder Task A."""


class TaskB(CoSyLuigiTask):
    """Placeholder Task B."""


class TaskC(CoSyLuigiTask):
    """Placeholder Task C."""


class TaskD(CoSyLuigiTask):
    """Placeholder Task D."""


class TaskE(CoSyLuigiTask):
    """Placeholder Task E."""


class TaskF(CoSyLuigiTask):
    """Placeholder Task F."""


class TaskG(CoSyLuigiTask):
    """Placeholder Task G."""


class TaskH(CoSyLuigiTask):
    """Placeholder Task H."""


class TaskI(CoSyLuigiTask):
    """Placeholder Task I."""


class TaskJ(CoSyLuigiTask):
    """ "Placeholder Task J."""


class TaskK(CoSyLuigiTask):
    """Placeholder Task K."""


def test_heterogeneous_repo_input():
    """Test if instantiating a CoSyLuigiRepo for a 2-times nested mixed Sequence of Sequences leads to the luigi_repo
    containing a set of the types contained in the Sequence of  CoSyLuigiTasks. The Sequence is passed through the
    varargs of the CoSyLuigiRepo constructor."""
    repo = CoSyLuigiRepo(TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK]))
    assert repo.luigi_repo == {TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK}


def test_heterogeneous_args_input():
    """Test if calling flatten on a 2-times nested mixed Sequence of Sequences returns a set containing the types
    contained in the Sequence of CoSyLuigiTasks. The Sequence is passed through the varargs of flatten."""
    flattened_collection = set(
        flatten(TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK]))
    )
    assert flattened_collection == {TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK}


def test_heterogeneous_list_input():
    """Test if calling flatten on a 2-times nested mixed list of Sequences returns a set containing the types
    contained in the list of CoSyLuigiTasks. The list is passed as a single argument to flatten."""
    flattened_collection = list(
        flatten([TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK])])
    )
    assert flattened_collection == [TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK]


def test_heterogeneous_tuple_input():
    """Test if calling flatten on a 2-times nested mixed tuple of Sequences returns a set containing the types
    contained in the tuple of CoSyLuigiTasks. The tuple is passed as a single argument to flatten."""
    flattened_collection = tuple(
        flatten((TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK])))
    )
    assert flattened_collection == (TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK)
