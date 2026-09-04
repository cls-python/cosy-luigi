"""Tests that the unique_in_prior_tasks constraint works as expected."""

import logging
from abc import ABC
from collections.abc import Callable, Sequence

import pytest

from maestro import Maestro, Repository, Task, TaskParameter
from maestro.constraints import is_unique_in_prior_tasks


class ScaleDataABC(Task, ABC):
    """Abstract class for Scalers."""


class ScaleData(ScaleDataABC):
    """Concrete class for Scalers."""


class ScaleDataVariantA(ScaleData):
    """A specific variant of a concrete scaler."""


class ScaleDataVariantB(ScaleData):
    """A specific variant of a concrete scaler."""


class TrainModel(Task, ABC):
    """Abstract class for training a model."""

    scaled_data = TaskParameter(ScaleDataABC)


class TrainModelVariantA(TrainModel):
    """A concrete model to train."""


class TrainModelVariantB(TrainModel):
    """A concrete model to train."""


class EvaluatePipelineWithUniqueScaler(Task):
    """Pipeline class that uses flag of TaskParameter to ensure that the same scaler is used throughout the pipeline."""

    train_model = TaskParameter(TrainModel)
    scaled_data = TaskParameter(ScaleDataABC, unique_across_prior_tasks=True)


class EvaluatePipelineWithConstraintUniqueScaler(Task):
    """Pipeline class that uses constraints.is_unique_in_prior_tasks to construct a constraint that ensures that the
    same scaler is used throughout the pipeline."""

    train_model = TaskParameter(TrainModel)
    scaled_data = TaskParameter(ScaleDataABC)

    @classmethod
    def constraints(cls) -> Sequence[Callable[..., bool]]:
        """Overrides the constraints method with a concrete constraint that ensures that the same subclass of
        ScaleDataABC is used throughout each pipeline.

        Returns:
            Sequence[Callable[..., bool]]: The constraints.
        """
        return [lambda vs: is_unique_in_prior_tasks(vs, ScaleDataABC)]


class EvaluatePipelineWithUniqueScalerAndNonAbstractSuper(Task):
    """Pipeline class that uses constraints.is_unique_in_prior_tasks to construct a constraint that ensures that the
    same scaler is used throughout the pipeline. Instead of an abstract class for the constraint, uses a concrete class
    with subclasses."""

    train_model = TaskParameter(TrainModel)
    scaled_data = TaskParameter(ScaleData, unique_across_prior_tasks=True)


class EvaluatePipeline(Task):
    """Pipeline class without constraints."""

    train_model = TaskParameter(TrainModel)
    scaled_data = TaskParameter(ScaleDataABC)


@pytest.fixture
def repo_without_constraints() -> Repository:
    """Constructs a Repository with no constraints.

    Returns:
        Repository: The Repository with no constraints.
    """
    return Repository(TrainModel, ScaleDataABC, EvaluatePipeline)


@pytest.fixture
def repo_with_constraints() -> Repository:
    """Constructs a Repository with constraints set on the TaskParameter.

    Returns:
        Repository: The Repository with constraints.
    """
    return Repository(TrainModel, ScaleDataABC, EvaluatePipelineWithUniqueScaler)


@pytest.fixture
def repo_with_manual_constraints() -> Repository:
    """Constructs a Repository with constraints set by overriding the constraints method.

    Returns:
        Repository: The Repository with constraints.
    """
    return Repository(TrainModel, ScaleDataABC, EvaluatePipelineWithConstraintUniqueScaler)


@pytest.fixture
def repo_with_non_abstract_super() -> Repository:
    """Constructs a Repository with constraints set on a non-abstract TaskParameter.

    Returns:
        Repository: The Repository with constraints.
    """
    return Repository(
        TrainModel, ScaleData, ScaleDataVariantA, ScaleDataVariantB, EvaluatePipelineWithUniqueScalerAndNonAbstractSuper
    )


def test_implementation_is_not_unique_across_prior_tasks(repo_without_constraints: Repository):
    """Tests that without constraints there are unwanted pipeline variations where the only variance is using scalers
    inconsistently.

    Args:
        repo_without_constraints (Repository): The Repository without constraints.
    """
    maestro = Maestro(
        repo_without_constraints.cls_repo,
        repo_without_constraints.taxonomy,
    )
    results = list(maestro.query(EvaluatePipeline.target()))
    assert len(results) == 18


def test_implementation_is_unique_across_prior_tasks(repo_with_constraints: Repository):
    """Test that with constraints applied to the TaskParameter there are no variants with inconsistent scaler usage.

    Args:
        repo_with_constraints (Repository): The Repository with constraints.
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
    repo_with_manual_constraints: Repository,
):
    """Test that with constraints applied by overriding the constraints method there are no variants with inconsistent
    scaler usage.

    Args:
        repo_with_manual_constraints (Repository): The Repository with constraints.
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
    repo_with_non_abstract_super: Repository,
):
    """Test that with constraints applied to the TaskParameter for a non-abstract class there are no variants with
    inconsistent scaler usage.

    Args:
        repo_with_non_abstract_super (Repository): The Repository with constraints.
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
    """Tests that the inspection whether constraints are used on something that can not exhibit variance works.

    Args:
        caplog (Generator[LogCaptureFixture, None, None]): Captures the logging output to verify inspector message.
    """
    caplog.set_level(logging.WARNING)
    repo_with_constraints_and_no_variance = Repository(
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
