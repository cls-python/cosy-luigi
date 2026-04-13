from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask
from cosy_luigi.utils import flatten


class TaskA(CoSyLuigiTask):
    pass


class TaskB(CoSyLuigiTask):
    pass


class TaskC(CoSyLuigiTask):
    pass


class TaskD(CoSyLuigiTask):
    pass


class TaskE(CoSyLuigiTask):
    pass


class TaskF(CoSyLuigiTask):
    pass


class TaskG(CoSyLuigiTask):
    pass


class TaskH(CoSyLuigiTask):
    pass


class TaskI(CoSyLuigiTask):
    pass


class TaskJ(CoSyLuigiTask):
    pass


class TaskK(CoSyLuigiTask):
    pass


def test_heterogeneous_repo_input():
    repo = CoSyLuigiRepo(TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK]))
    assert repo.luigi_repo == {TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK}

def test_heterogeneous_args_input():
    flattened_collection = set(
        flatten(TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK]))
    )
    assert flattened_collection == {TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK}

def test_heterogeneous_list_input():
    flattened_collection = list(
        flatten([TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK])])
    )
    assert flattened_collection == [TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK]

def test_heterogeneous_tuple_input():
    flattened_collection = tuple(
        flatten((TaskA, [TaskB, TaskC], (TaskD, TaskE), [TaskF, (TaskG, TaskH)], (TaskI, [TaskJ, TaskK])))
    )
    assert flattened_collection == (TaskA, TaskB, TaskC, TaskD, TaskE, TaskF, TaskG, TaskH, TaskI, TaskJ, TaskK)