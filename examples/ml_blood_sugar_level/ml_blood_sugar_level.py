import os
from abc import ABC

import numpy as np
import pandas as pd
import skops.io as sio
from sklearn.base import RegressorMixin
from sklearn.datasets import load_diabetes
from sklearn.linear_model import LassoLars, LinearRegression
from sklearn.metrics import root_mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, RobustScaler

from maestro import LocalTarget, Maestro, Repository, Task, TaskParameter, run_pipelines

ninetydegaisle = True


class LoadDiabetesData(Task):
    def output(self):
        return {"diabetes_data": LocalTarget("data/diabetes.json")}

    def run(self):
        diabetes = load_diabetes()
        df = pd.DataFrame(
            data=np.c_[diabetes["data"], diabetes["target"]], columns=diabetes["feature_names"] + ["target"]
        )
        os.makedirs("data", exist_ok=True)
        df.to_json(self.output()["diabetes_data"].path)


class TrainTestSplit(Task):
    diabetes = TaskParameter(LoadDiabetesData)

    def output(self):
        return {
            "x_train": LocalTarget("data/x_train.json"),
            "x_test": LocalTarget("data/x_test.json"),
            "y_train": LocalTarget("data/y_train.json"),
            "y_test": LocalTarget("data/y_test.json"),
        }

    def run(self):
        data = pd.read_json(self.input()["diabetes"]["diabetes_data"].path)
        x = data.drop(["target"], axis="columns")
        y = data[["target"]]
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.33, random_state=42)

        x_train.to_json(self.output()["x_train"].path)
        x_test.to_json(self.output()["x_test"].path)
        y_train.to_json(self.output()["y_train"].path)
        y_test.to_json(self.output()["y_test"].path)


class FitTransformScaler(Task, ABC):
    splitted_data = TaskParameter(TrainTestSplit)
    scaler_name: str
    scaler: MinMaxScaler | RobustScaler

    def output(self):
        return {
            "scaled_x_train": LocalTarget(f"data/{self.scaler_name}_scaled_x_train.json"),
            "scaled_x_test": LocalTarget(f"data/{self.scaler_name}_scaled_x_test.json"),
            "scaler": LocalTarget(f"data/{self.scaler_name}_scaler.skops"),
        }

    def scale(self, data_identifier: str):
        x = pd.read_json(self.input()["splitted_data"][data_identifier].path)
        self.scaler.fit(x)
        x_train = pd.DataFrame(self.scaler.transform(x), columns=self.scaler.feature_names_in_, index=x.index)
        x_train.to_json(self.output()[f"scaled_{data_identifier}"].path)

    def run(self):
        self.scale("x_train")
        self.scale("x_test")

        with open(self.output()["scaler"].path, "wb") as outfile:
            sio.dump(self.scaler, outfile)


class FitTransformMinMaxScaler(FitTransformScaler):
    scaler_name = "minmax"
    scaler = MinMaxScaler()


class FitTransformRobustScaler(FitTransformScaler):
    scaler_name = "robust"
    scaler = RobustScaler()


class TrainRegressionModel(Task, ABC):
    scaled_feats = TaskParameter(FitTransformScaler)
    splitted_data = TaskParameter(TrainTestSplit)
    model: RegressorMixin

    def output(self):
        return {"model": LocalTarget(f"data/{self.variant_label}.skops")}

    def run(self):
        x_train = pd.read_json(self.input()["scaled_feats"]["scaled_x_train"].path)
        y_train = pd.read_json(self.input()["splitted_data"]["y_train"].path)

        self.model.fit(x_train, y_train)

        sio.dump(self.model, self.output()["model"].path)


class TrainLinearRegressionModel(TrainRegressionModel):
    model = LinearRegression()


class TrainLassoLarsModel(TrainRegressionModel):
    model = LassoLars()


class EvaluateRegressionModel(Task):
    regressor = TaskParameter(TrainRegressionModel)
    scaled_feats = TaskParameter(FitTransformScaler, unique_across_prior_tasks=True)
    splitted_data = TaskParameter(TrainTestSplit)

    def output(self):
        return {"evaluation": LocalTarget(f"data/y_pred-{self.variant_label}.json")}

    def run(self):
        unknown_types = sio.get_untrusted_types(file=self.input()["regressor"]["model"].path)
        reg = sio.load(self.input()["regressor"]["model"].path, trusted=unknown_types)

        scaled_x_test = pd.read_json(self.input()["scaled_feats"]["scaled_x_test"].path)
        y_pred = pd.DataFrame()
        y_pred["y_pred"] = reg.predict(scaled_x_test).ravel()
        y_pred.to_json(self.output()["evaluation"].path)


def main():
    repo = Repository(
        TrainTestSplit,
        LoadDiabetesData,
        FitTransformRobustScaler,
        FitTransformMinMaxScaler,
        TrainLinearRegressionModel,
        TrainLassoLarsModel,
        EvaluateRegressionModel,
    )
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    results = maestro.query(EvaluateRegressionModel.target())
    run_pipelines(results).report()


if __name__ == "__main__":
    main()
