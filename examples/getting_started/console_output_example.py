import shutil
import time
from abc import ABC
from pathlib import Path

from maestro import LocalTarget, Maestro, Repository, Task, TaskParameter, run_pipelines
from maestro.core.execution import coloured_task_name

OUTPUT = Path("output/console_output_example")


class Step(Task, ABC):
    seconds = 1.6

    def output(self):
        return {"result": LocalTarget(OUTPUT / f"{self.variant_label}.txt")}

    def run(self):
        time.sleep(self.seconds)
        with self.output()["result"].open("w") as result:
            result.write(f"{type(self).__name__} finished")


class LoadData(Step):
    pass


class PreProcess(Step):
    load = TaskParameter(LoadData)


class ExtractFeatures(Step):
    cleaned = TaskParameter(PreProcess)


class ExtractLabels(Step):
    cleaned = TaskParameter(PreProcess)


class Split(Step):
    seconds = 2.4
    features = TaskParameter(ExtractFeatures)
    labels = TaskParameter(ExtractLabels)


class Model(Step, ABC):
    split = TaskParameter(Split)


class WorkingModel(Model):
    seconds = 3.0


class BrokenModelTuner(Step):
    split = TaskParameter(Split)

    def run(self):
        time.sleep(self.seconds)
        msg = "Oh no! This Tuner is intentionally broken!"
        raise ValueError(msg)


class LoadExtraData(Step):
    def run(self):
        time.sleep(self.seconds)
        msg = "You don't have extra data!"
        raise FileNotFoundError(msg)


class ExtraData(Step):
    baseline = TaskParameter(LoadExtraData)


class BlockedModel(Model):
    tuning = TaskParameter(BrokenModelTuner)
    comparison = TaskParameter(ExtraData)


class Evaluate(Step):
    model = TaskParameter(Model)
    split = TaskParameter(Split)


def main():
    shutil.rmtree(OUTPUT, ignore_errors=True)
    repo = Repository(
        Evaluate,
        Model,
        Split,
        ExtractFeatures,
        ExtractLabels,
        PreProcess,
        LoadData,
        BrokenModelTuner,
        ExtraData,
        LoadExtraData,
    )
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    results = maestro.query(Evaluate.target())

    print(coloured_task_name("\n####### Cold Run #######\n", "SKIPPED"))  # pretty
    run_pipelines(results, workers=2).report()
    print(coloured_task_name("\n####### Warm Run #######\n", "BLOCKED"))  # pretty
    run_pipelines(results, workers=2).report()


if __name__ == "__main__":
    main()
