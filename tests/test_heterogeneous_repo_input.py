"""_summary_."""

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask
from cosy_luigi.utils import flatten


class TaskA(CoSyLuigiTask):
    """_summary_."""


class TaskB(CoSyLuigiTask):
    """_summary_."""


class TaskC(CoSyLuigiTask):
    """_summary_."""


class TaskD(CoSyLuigiTask):
    """_summary_."""


class TaskE(CoSyLuigiTask):
    """_summary_."""


class TaskF(CoSyLuigiTask):
    """_summary_."""


class TaskG(CoSyLuigiTask):
    """_summary_."""


class TaskH(CoSyLuigiTask):
    """_summary_."""


class TaskI(CoSyLuigiTask):
    """_summary_."""


class TaskJ(CoSyLuigiTask):
    """_summary_."""


class TaskK(CoSyLuigiTask):
    """_summary_."""


def test_heterogeneous_repo_input():
    """_summary_."""
    repo = CoSyLuigiRepo(TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK]))
    assert repo.luigi_repo == {TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK}


def test_heterogeneous_args_input():
    """_summary_."""
    flattened_collection = set(
        flatten(TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK]))
    )
    assert flattened_collection == {TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK}


def test_heterogeneous_list_input():
    """_summary_."""
    flattened_collection = list(
        flatten([TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK])])
    )
    assert flattened_collection == [TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK]


def test_heterogeneous_tuple_input():
    """_summary_."""
    flattened_collection = tuple(
        flatten((TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK])))
    )
    assert flattened_collection == (TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK)
