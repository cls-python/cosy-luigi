from cosy_luigi.combinatorics import CoSyLuigiRepo, CoSyLuigiTask


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
