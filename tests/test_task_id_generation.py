"""_summary_."""

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
    """_summary_."""

    identifier: str


class ShadedA(Shaded):
    """_summary_."""

    identifier = "A"


class ShadedB(Shaded):
    """_summary_."""

    identifier = "B"


class Shade(CoSyLuigiTask):
    """_summary_."""

    shaded = CoSyLuigiTaskParameter(Shaded)

    def complete(self):
        """_summary_.

        Returns:
            _type_: _description_
        """
        return True


class Evaluate(CoSyLuigiTask):
    """_summary_."""

    shade = CoSyLuigiTaskParameter(Shade)

    def run(self):
        """_summary_."""
        with self.output()["output"].open("w") as f:
            f.write("OK.")

    def output(self):
        """_summary_.

        Returns:
            _type_: _description_
        """
        return {"output": MockTarget(self.shade.shaded.identifier)}


class EvaluateWithPotentialToShade(Evaluate):
    """_summary_.

    Attributes:
        task_id (_type_): _description_
    """

    def __init__(self, *args, **kwargs):
        """_summary_.

        Args:
            *args (_type_): _description_
            **kwargs (_type_): _description_
        """
        super().__init__(*args, **kwargs)
        # Copy the behaviour of regular Luigi
        self.task_id = task_id_str(self.get_task_family(), self.to_str_params(only_significant=True, only_public=True))
        self.__hash = hash(self.task_id)


@pytest.fixture
def repo():
    """_summary_.

    Returns:
        _type_: _description_
    """
    return CoSyLuigiRepo(Evaluate, Shade, Shaded)


@pytest.fixture
def shadeable_repo():
    """_summary_.

    Returns:
        _type_: _description_
    """
    return CoSyLuigiRepo(EvaluateWithPotentialToShade, Shade, Shaded)


def test_shading_not_possible(repo):
    """_summary_.

    Args:
        repo (_type_): _description_
    """
    fs.clear()
    assert not target_a.exists()
    assert not target_b.exists()
    maestro = Maestro(
        repo.cls_repo,
        repo.taxonomy,
    )
    luigi.build(list(maestro.query(Evaluate.target())), local_scheduler=True, detailed_summary=True)
    assert target_a.exists()
    assert target_b.exists()


def test_shading_would_be_possible(shadeable_repo):
    """_summary_.

    Args:
        shadeable_repo (_type_): _description_
    """
    fs.clear()
    assert not target_a.exists()
    assert not target_b.exists()
    maestro = Maestro(
        shadeable_repo.cls_repo,
        shadeable_repo.taxonomy,
    )
    luigi.build(list(maestro.query(EvaluateWithPotentialToShade.target())), local_scheduler=True, detailed_summary=True)
    assert not (target_a.exists() and target_b.exists())


def test_output_mapping_is_enforced():
    """_summary_."""

    class TaskWithWrongOutputA(CoSyLuigiTask):
        """_summary_."""

        def output(self):
            """_summary_.

            Returns:
                _type_: _description_
            """
            return MockTarget("")

    with pytest.raises(TypeError):
        TaskWithWrongOutputA()

    class TaskWithWrongOutputB(CoSyLuigiTask):
        """_summary_."""

        def output(self):
            """_summary_.

            Returns:
                _type_: _description_
            """
            return [MockTarget("")]

    with pytest.raises(TypeError):
        TaskWithWrongOutputB()

    class TaskWithNoneOutput(CoSyLuigiTask):
        """_summary_."""

    TaskWithNoneOutput()
