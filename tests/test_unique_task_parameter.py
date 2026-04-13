import logging
from abc import ABC
from collections.abc import Callable, Sequence

import pytest
from cosy.maestro import Maestro

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter
from cosy_luigi.constraints import is_unique_in_prior_tasks


class ScaleDataABC(CoSyLuigiTask, ABC):
    pass


class ScaleData(ScaleDataABC):
    pass


class ScaleDataVariantA(ScaleData):
    pass


class ScaleDataVariantB(ScaleData):
    pass


class TrainModel(CoSyLuigiTask, ABC):
    scaled_data = CoSyLuigiTaskParameter(ScaleDataABC)


class TrainModelVariantA(TrainModel):
    pass


class TrainModelVariantB(TrainModel):
    pass


class EvaluatePipelineWithUniqueScaler(CoSyLuigiTask):
    train_model = CoSyLuigiTaskParameter(TrainModel)
    scaled_data = CoSyLuigiTaskParameter(ScaleDataABC, unique_across_prior_tasks=True)


class EvaluatePipelineWithConstraintUniqueScaler(CoSyLuigiTask):
    train_model = CoSyLuigiTaskParameter(TrainModel)
    scaled_data = CoSyLuigiTaskParameter(ScaleDataABC)

    @classmethod
    def constraints(cls) -> Sequence[Callable[..., bool]]:
        return [lambda vs: is_unique_in_prior_tasks(vs, ScaleDataABC)]


class EvaluatePipelineWithUniqueScalerAndNonAbstractSuper(CoSyLuigiTask):
    train_model = CoSyLuigiTaskParameter(TrainModel)
    scaled_data = CoSyLuigiTaskParameter(ScaleData, unique_across_prior_tasks=True)


class EvaluatePipeline(CoSyLuigiTask):
    train_model = CoSyLuigiTaskParameter(TrainModel)
    scaled_data = CoSyLuigiTaskParameter(ScaleDataABC)


@pytest.fixture
def repo_without_constraints():
    return CoSyLuigiRepo(TrainModel, ScaleDataABC, EvaluatePipeline)


@pytest.fixture
def repo_with_constraints():
    return CoSyLuigiRepo(TrainModel, ScaleDataABC, EvaluatePipelineWithUniqueScaler)


@pytest.fixture
def repo_with_manual_constraints():
    return CoSyLuigiRepo(TrainModel, ScaleDataABC, EvaluatePipelineWithConstraintUniqueScaler)


@pytest.fixture
def repo_with_non_abstract_super():
    return CoSyLuigiRepo(
        TrainModel, ScaleData, ScaleDataVariantA, ScaleDataVariantB, EvaluatePipelineWithUniqueScalerAndNonAbstractSuper
    )


def test_implementation_is_not_unique_across_prior_tasks(repo_without_constraints):
    maestro = Maestro(
        repo_without_constraints.cls_repo,
        repo_without_constraints.taxonomy,
    )
    results = list(maestro.query(EvaluatePipeline.target()))
    assert len(results) == 18


def test_implementation_is_unique_across_prior_tasks(repo_with_constraints):
    maestro = Maestro(
        repo_with_constraints.cls_repo,
        repo_with_constraints.taxonomy,
    )
    results: list[EvaluatePipelineWithUniqueScaler] = list(maestro.query(EvaluatePipelineWithUniqueScaler.target()))
    assert len(results) == 6
    for result in results:
        assert result.scaled_data == result.train_model.scaled_data


def test_implementation_is_unique_across_prior_tasks_with_manual_constraint(repo_with_manual_constraints):
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


def test_implementation_is_unique_across_prior_tasks_with_non_abstract_super(repo_with_non_abstract_super):
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
