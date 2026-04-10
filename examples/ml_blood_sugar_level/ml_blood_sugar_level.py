import json
import textwrap
from abc import ABC
from pathlib import Path

import luigi
import numpy as np
import pandas as pd
import skops.io as sio
from cosy.maestro import Maestro
from sklearn.datasets import load_diabetes
from sklearn.linear_model import LassoLars, LinearRegression
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


class FitTransformMinMaxScaler(FitTransformScaler):
    def output(self):
        return {
            "scaled_x_train": luigi.LocalTarget("minmax_scaled_x_train.json"),
            "scaled_x_test": luigi.LocalTarget("minmax_scaled_x_test.json"),
            "scaler": luigi.LocalTarget("minmax_scaler.skops"),
        }

    def run(self):
        print(self.input())
        x_train = pd.read_json(self.input()["splitted_data"]["x_train"].path)
        scaler = MinMaxScaler()
        scaler.fit(x_train)
        scaled_x_train = pd.DataFrame(scaler.transform(x_train), columns=scaler.feature_names_in_, index=x_train.index)
        scaled_x_train.to_json(self.output()["scaled_x_train"].path)

        x_test = pd.read_json(self.input()["splitted_data"]["x_test"].path)
        scaler.transform(x_test)
        scaled_x_test = pd.DataFrame(scaler.transform(x_test), columns=scaler.feature_names_in_, index=x_test.index)
        scaled_x_test.to_json(self.output()["scaled_x_test"].path)

        with open(self.output()["scaler"].path, "wb") as outfile:
            sio.dump(scaler, outfile)


class FitTransformRobustScaler(FitTransformScaler):
    def output(self):
        return {
            "scaled_x_train": luigi.LocalTarget("robust_scaled_x_train.json"),
            "scaled_x_test": luigi.LocalTarget("robust_scaled_x_test.json"),
            "scaler": luigi.LocalTarget("robust_scaler.json"),
        }

    def run(self):
        x_train = pd.read_json(self.input()["splitted_data"]["x_train"].path)
        scaler = RobustScaler()
        scaler.fit(x_train)
        scaled_x_train = pd.DataFrame(scaler.transform(x_train), columns=scaler.feature_names_in_, index=x_train.index)
        scaled_x_train.to_json(self.output()["scaled_x_train"].path)

        x_test = pd.read_json(self.input()["splitted_data"]["x_test"].path)
        scaler.transform(x_test)
        scaled_x_test = pd.DataFrame(scaler.transform(x_test), columns=scaler.feature_names_in_, index=x_test.index)
        scaled_x_test.to_json(self.output()["scaled_x_test"].path)

        with open(self.output()["scaler"].path, "wb") as outfile:
            json.dump(scaler, outfile)


class TrainRegressionModel(CoSyLuigiTask, ABC):
    scaled_feats = CoSyLuigiTaskParameter(FitTransformScaler)
    splitted_data = CoSyLuigiTaskParameter(TrainTestSplit)

    def _get_variant_label(self):
        return Path(self.input()["scaled_feats"]["scaled_x_train"].path).stem


class TrainLinearRegressionModel(TrainRegressionModel):
    def output(self):
        return {"model": luigi.LocalTarget("linear_reg" + "-" + self._get_variant_label() + ".skops")}

    def run(self):
        x_train = pd.read_json(self.input()["scaled_feats"]["scaled_x_train"].path)
        y_train = pd.read_json(self.input()["splitted_data"]["y_train"].path)

        reg = LinearRegression()
        reg.fit(x_train, y_train)

        sio.dump(reg, self.output()["model"].path)


class TrainLassoLarsModel(TrainRegressionModel):
    def output(self):
        return {"model": luigi.LocalTarget("lasso_lars" + "-" + self._get_variant_label() + ".skops")}

    def run(self):
        x_train = pd.read_json(self.input()["scaled_feats"]["scaled_x_train"].path)
        y_train = pd.read_json(self.input()["splitted_data"]["y_train"].path)

        reg = LassoLars()
        reg.fit(x_train, y_train)
        sio.dump(reg, self.output()["model"].path)


class EvaluateRegressionModel(CoSyLuigiTask):
    regressor = CoSyLuigiTaskParameter(TrainRegressionModel)
    scaled_feats = CoSyLuigiTaskParameter(FitTransformMinMaxScaler, unique_across_prior_tasks=True)
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
