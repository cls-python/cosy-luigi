"""_summary_."""
import logging
from abc import ABC
from collections.abc import Callable, Sequence

import pytest
from cosy.maestro import Maestro

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter
from cosy_luigi.constraints import is_unique_in_prior_tasks


class ScaleDataABC(CoSyLuigiTask, ABC):
    """_summary_."""
    pass


class ScaleData(ScaleDataABC):
    """_summary_."""
    pass


class ScaleDataVariantA(ScaleData):
    """_summary_."""
    pass


class ScaleDataVariantB(ScaleData):
    """_summary_."""
    pass


class TrainModel(CoSyLuigiTask, ABC):
    """_summary_."""
    scaled_data = CoSyLuigiTaskParameter(ScaleDataABC)


class TrainModelVariantA(TrainModel):
    """_summary_."""
    pass


class TrainModelVariantB(TrainModel):
    """_summary_."""
    pass


class EvaluatePipelineWithUniqueScaler(CoSyLuigiTask):
    """_summary_."""
    train_model = CoSyLuigiTaskParameter(TrainModel)
    scaled_data = CoSyLuigiTaskParameter(ScaleDataABC, unique_across_prior_tasks=True)


class EvaluatePipelineWithConstraintUniqueScaler(CoSyLuigiTask):
    """_summary_."""
    train_model = CoSyLuigiTaskParameter(TrainModel)
    scaled_data = CoSyLuigiTaskParameter(ScaleDataABC)

    @classmethod
    def constraints(cls) -> Sequence[Callable[..., bool]]:
        """_summary_.

        Returns:
            Sequence[Callable[..., bool]]: _description_
        """
        return [lambda vs: is_unique_in_prior_tasks(vs, ScaleDataABC)]


class EvaluatePipelineWithUniqueScalerAndNonAbstractSuper(CoSyLuigiTask):
    """_summary_."""
    train_model = CoSyLuigiTaskParameter(TrainModel)
    scaled_data = CoSyLuigiTaskParameter(ScaleData, unique_across_prior_tasks=True)


class EvaluatePipeline(CoSyLuigiTask):
    """_summary_."""
    train_model = CoSyLuigiTaskParameter(TrainModel)
    scaled_data = CoSyLuigiTaskParameter(ScaleDataABC)


@pytest.fixture
def repo_without_constraints() -> CoSyLuigiRepo:
    """_summary_.

    Returns:
        CoSyLuigiRepo: _description_
    """
    return CoSyLuigiRepo(TrainModel, ScaleDataABC, EvaluatePipeline)


@pytest.fixture
def repo_with_constraints() -> CoSyLuigiRepo:
    """_summary_.

    Returns:
        CoSyLuigiRepo: _description_
    """
    return CoSyLuigiRepo(TrainModel, ScaleDataABC, EvaluatePipelineWithUniqueScaler)


@pytest.fixture
def repo_with_manual_constraints() -> CoSyLuigiRepo:
    """_summary_.

    Returns:
        CoSyLuigiRepo: _description_
    """
    return CoSyLuigiRepo(TrainModel, ScaleDataABC, EvaluatePipelineWithConstraintUniqueScaler)


@pytest.fixture
def repo_with_non_abstract_super() -> CoSyLuigiRepo:
    """_summary_.

    Returns:
        CoSyLuigiRepo: _description_
    """
    return CoSyLuigiRepo(
        TrainModel, ScaleData, ScaleDataVariantA, ScaleDataVariantB, EvaluatePipelineWithUniqueScalerAndNonAbstractSuper
    )


def test_implementation_is_not_unique_across_prior_tasks(repo_without_constraints: CoSyLuigiRepo):
    """_summary_.

    Args:
        repo_without_constraints (CoSyLuigiRepo): _description_
    """
    maestro = Maestro(
        repo_without_constraints.cls_repo,
        repo_without_constraints.taxonomy,
    )
    results = list(maestro.query(EvaluatePipeline.target()))
    assert len(results) == 18


def test_implementation_is_unique_across_prior_tasks(repo_with_constraints: CoSyLuigiRepo):
    """_summary_.

    Args:
        repo_with_constraints (CoSyLuigiRepo): _description_
    """
    maestro = Maestro(
        repo_with_constraints.cls_repo,
        repo_with_constraints.taxonomy,
    )
    results: list[EvaluatePipelineWithUniqueScaler] = list(maestro.query(EvaluatePipelineWithUniqueScaler.target()))
    assert len(results) == 6
    for result in results:
        assert result.scaled_data == result.train_model.scaled_data


def test_implementation_is_unique_across_prior_tasks_with_manual_constraint(
    repo_with_manual_constraints: CoSyLuigiRepo,
):
    """_summary_.

    Args:
        repo_with_manual_constraints (CoSyLuigiRepo): _description_
    """
    maestro = Maestro(
        repo_with_manual_constraints.cls_repo,
        repo_with_manual_constraints.taxonomy,
    )
    results: list[EvaluatePipelineWithConstraintUniqueScaler] = list(
        maestro.query(EvaluatePipelineWithConstraintUniqueScaler.target())
    )
    assert len(results) == 6
    for result in results:
        assert result.scaled_data == result.train_model.scaled_data


def test_implementation_is_unique_across_prior_tasks_with_non_abstract_super(
    repo_with_non_abstract_super: CoSyLuigiRepo,
):
    """_summary_.

    Args:
        repo_with_non_abstract_super (CoSyLuigiRepo): _description_
    """
    maestro = Maestro(
        repo_with_non_abstract_super.cls_repo,
        repo_with_non_abstract_super.taxonomy,
    )
    results: list[EvaluatePipelineWithUniqueScalerAndNonAbstractSuper] = list(
        maestro.query(EvaluatePipelineWithUniqueScalerAndNonAbstractSuper.target())
    )
    assert len(results) == 6
    for result in results:
        assert result.scaled_data == result.train_model.scaled_data


def test_warning_if_unique_across_prior_tasks_but_no_variance(caplog):
    """_summary_.

    Args:
        caplog (_type_): _description_
    """
    caplog.set_level(logging.WARNING)
    repo_with_constraints_and_no_variance = CoSyLuigiRepo(
        TrainModel, ScaleData, EvaluatePipelineWithUniqueScalerAndNonAbstractSuper
    )
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "ScaleData" in caplog.records[0].message

    maestro = Maestro(
        repo_with_constraints_and_no_variance.cls_repo,
        repo_with_constraints_and_no_variance.taxonomy,
    )
    results = list(maestro.query(EvaluatePipelineWithUniqueScalerAndNonAbstractSuper.target()))
    assert len(results) == 2
