"""_summary_."""
from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask
from cosy_luigi.utils import flatten


class TaskA(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskB(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskC(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskD(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskE(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskF(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskG(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskH(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskI(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskJ(CoSyLuigiTask):
    """_summary_."""
    pass


class TaskK(CoSyLuigiTask):
    """_summary_."""
    pass


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
