import textwrap
from abc import ABC
from pathlib import Path

import luigi
import numpy as np
import pandas as pd
import skops.io as sio
from cosy.maestro import Maestro
from sklearn.base import RegressorMixin
from sklearn.datasets import load_diabetes
from sklearn.linear_model import LassoLars
from sklearn.metrics import root_mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, RobustScaler

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter

ninetydegaisle = True


class LoadDiabetesData(CoSyLuigiTask):
    def output(self):
        return {"diabetes_data": luigi.LocalTarget("diabetes.json")}

    def run(self):
        diabetes = load_diabetes()
        df = pd.DataFrame(
            data=np.c_[diabetes["data"], diabetes["target"]], columns=diabetes["feature_names"] + ["target"]
        )

        df.to_json(self.output()["diabetes_data"].path)


class TrainTestSplit(CoSyLuigiTask):
    diabetes = CoSyLuigiTaskParameter(LoadDiabetesData)

    def output(self):
        return {
            "x_train": luigi.LocalTarget("x_train.json"),
            "x_test": luigi.LocalTarget("x_test.json"),
            "y_train": luigi.LocalTarget("y_train.json"),
            "y_test": luigi.LocalTarget("y_test.json"),
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


class FitTransformScaler(CoSyLuigiTask, ABC):
    splitted_data = CoSyLuigiTaskParameter(TrainTestSplit)
    scaler_name: str
    scaler: MinMaxScaler | RobustScaler

    def output(self):
        return {
            "scaled_x_train": luigi.LocalTarget(f"{self.scaler_name}_scaled_x_train.json"),
            "scaled_x_test": luigi.LocalTarget(f"{self.scaler_name}_scaled_x_test.json"),
            "scaler": luigi.LocalTarget(f"{self.scaler_name}_scaler.skops"),
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


class TrainRegressionModel(CoSyLuigiTask, ABC):
    scaled_feats = CoSyLuigiTaskParameter(FitTransformScaler)
    splitted_data = CoSyLuigiTaskParameter(TrainTestSplit)
    model_name: str
    model: RegressorMixin

    def _get_variant_label(self):
        return f"{self.model_name}-{Path(self.input()['scaled_feats']['scaled_x_train'].path).stem}"

    def output(self):
        return {"model": luigi.LocalTarget(self._get_variant_label() + ".skops")}

    def run(self):
        x_train = pd.read_json(self.input()["scaled_feats"]["scaled_x_train"].path)
        y_train = pd.read_json(self.input()["splitted_data"]["y_train"].path)

        self.model.fit(x_train, y_train)

        sio.dump(self.model, self.output()["model"].path)


class TrainLinearRegressionModel(TrainRegressionModel):
    model_name = "linear_reg"


class TrainLassoLarsModel(TrainRegressionModel):
    model_name = "lasso_lars"

    def run(self):
        x_train = pd.read_json(self.input()["scaled_feats"]["scaled_x_train"].path)
        y_train = pd.read_json(self.input()["splitted_data"]["y_train"].path)

        reg = LassoLars()
        reg.fit(x_train, y_train)
        sio.dump(reg, self.output()["model"].path)


class EvaluateRegressionModel(CoSyLuigiTask):
    regressor = CoSyLuigiTaskParameter(TrainRegressionModel)
    scaled_feats = CoSyLuigiTaskParameter(FitTransformScaler, unique_across_prior_tasks=True)
    splitted_data = CoSyLuigiTaskParameter(TrainTestSplit)

    def _get_variant_label(self):
        return Path(self.input()["regressor"]["model"].path).stem

    def output(self):
        return luigi.LocalTarget("y_pred" + "-" + self._get_variant_label() + ".json")

    def run(self):
        unknown_types = sio.get_untrusted_types(file=self.input()["regressor"]["model"].path)
        reg = sio.load(self.input()["regressor"]["model"].path, trusted=unknown_types)

        scaled_x_test = pd.read_json(self.input()["scaled_feats"]["scaled_x_test"].path)
        y_test = pd.read_json(self.input()["splitted_data"]["y_test"].path)
        y_pred = pd.DataFrame()
        y_pred["y_pred"] = reg.predict(scaled_x_test).ravel()
        rmse = round(root_mean_squared_error(y_test, y_pred), 3)

        print(self._get_variant_label())
        print(f"RMSE: {rmse}")

        y_pred.to_json(self.output().path)


def main():
    repo = CoSyLuigiRepo(
        TrainTestSplit,
        LoadDiabetesData,
        FitTransformRobustScaler,
        FitTransformMinMaxScaler,
        TrainLinearRegressionModel,
        TrainLassoLarsModel,
        EvaluateRegressionModel,
    )
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    results = list(maestro.query(EvaluateRegressionModel.target()))
    luigi.build(results, local_scheduler=True, detailed_summary=True)
    print(
        textwrap.dedent(
            f"""
                ===============================================
                    There are a total of {len(results)} results
                ==============================================="""
        )
    )


if __name__ == "__main__":
    main()
