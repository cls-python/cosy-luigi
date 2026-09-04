"""Checks synthesis engines for equivalence."""

import pytest

from cosy_luigi import Repository, Task
from cosy_luigi.core.cosy_adapter import CoSyMaestro
from cosy_luigi.core.synthesis import Maestro
from tests.test_infinite_chain import ChainLink, RepeatingLink
from tests.test_task_id_generation import Evaluate, EvaluateWithPotentialToShade, Shade, Shaded
from tests.test_unique_task_parameter import (
    EvaluatePipeline,
    EvaluatePipelineWithConstraintUniqueScaler,
    EvaluatePipelineWithUniqueScaler,
    EvaluatePipelineWithUniqueScalerAndNonAbstractSuper,
    ScaleData,
    ScaleDataABC,
    ScaleDataVariantA,
    ScaleDataVariantB,
    TrainModel,
)


@pytest.mark.parametrize(
    ("tasks", "target", "max_count"),
    [
        ((TrainModel, ScaleDataABC, EvaluatePipeline), EvaluatePipeline, None),
        ((TrainModel, ScaleDataABC, EvaluatePipelineWithUniqueScaler), EvaluatePipelineWithUniqueScaler, None),
        (
            (TrainModel, ScaleDataABC, EvaluatePipelineWithConstraintUniqueScaler),
            EvaluatePipelineWithConstraintUniqueScaler,
            None,
        ),
        (
            (
                TrainModel,
                ScaleData,
                ScaleDataVariantA,
                ScaleDataVariantB,
                EvaluatePipelineWithUniqueScalerAndNonAbstractSuper,
            ),
            EvaluatePipelineWithUniqueScalerAndNonAbstractSuper,
            None,
        ),
        ((Evaluate, Shade, Shaded), Evaluate, None),
        ((EvaluateWithPotentialToShade, Shade, Shaded), EvaluateWithPotentialToShade, None),
        ((ChainLink,), RepeatingLink, 10),
    ],
    ids=["unconstrained", "unique", "manual-constraint", "non-abstract-super", "shading", "shadeable", "chain"],
)
def test_engines_agree(tasks: tuple[type[Task], ...], target: type[Task], max_count: int | None):
    """Tests that both engines find the same pipelines.

    Args:
        tasks (tuple[type[Task], ...]): The tasks in repository.
        target (type[Task]): Synthesis target.
        max_count (int | None):
    """
    repo = Repository(tasks)
    ours, cosys = (
        sorted(
            task.task_id for task in engine(repo.cls_repo, repo.taxonomy).query(target.target(), max_count=max_count)
        )
        for engine in (Maestro, CoSyMaestro)
    )
    assert ours == cosys
