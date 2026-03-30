import json
import os
from abc import ABC, abstractmethod
from pathlib import Path

import luigi
import pandas as pd
from cosy.maestro import Maestro
from lot_optimizers.groff_heuristic import GroffHeuristic
from lot_optimizers.least_unit_cost_method import LeastUnitCostMethod
from lot_optimizers.part_period_heuristic import PartPeriod
from lot_optimizers.silver_meal_heuristic import SilverMeal
from lot_optimizers.wagner_whitin import WagnerWhitin

from cosy_luigi.combinatorics import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter


class GetCosts(CoSyLuigiTask):
    def output(self):
        return {"costs": luigi.LocalTarget("data/costs.json")}

    def run(self):
        d = {
            "fixedCost": 400,  # Bestellkosten
            "varCost": 1,  # Lagerhaltungssatz
        }
        os.makedirs("data", exist_ok=True)
        with open(self.output()["costs"].path, "w") as f:
            json.dump(d, f, indent=4)


class GetHistoricDemand(CoSyLuigiTask):
    def output(self):
        return {"historic_demand": luigi.LocalTarget("data/historic_demand.csv")}

    def run(self):
        with self.output()["historic_demand"].open("w") as f:
            f.write("1, 5, 7, 8, 9, 10, 14, 16, 19, 21, 19, 23, 24, 26, 26, 26, 28, 26, 28, 30")


class PredictDemand(CoSyLuigiTask):
    get_historic_demand = CoSyLuigiTaskParameter(GetHistoricDemand)
    prediction_horizon = 8
    output_filename: str = ""

    def output(self):
        return {"predicted_demand": luigi.LocalTarget(self.output_filename)}


class PredictDemandByAverage(PredictDemand):
    output_filename = "data/predicted_demand_by_average.json"

    def run(self):
        with self.input()["get_historic_demand"]["historic_demand"].open() as infile:
            text = infile.read()
            historic_demand = [int(t) for t in text.split(",")]
            avg = int(sum(historic_demand) / len(historic_demand) + 0.5)
            predicted = [avg for _ in range(self.prediction_horizon)]
            data = {"predicted_demand": predicted}
            df_predicted = pd.DataFrame(data)

            df_predicted.to_json(self.output()["predicted_demand"].path)


class PredictDemandByLinearRegression(PredictDemand):
    output_filename = "data/predicted_demand_by_linear_regression.json"

    def run(self):
        with self.input()["get_historic_demand"]["historic_demand"].open():
            # Mocked for sake of example
            predicted = [10 + i for i in range(self.prediction_horizon)]
            data = {"predicted_demand": predicted}
            df_predicted = pd.DataFrame(data)

            df_predicted.to_json(self.output()["predicted_demand"].path)


class OptimizeLots(CoSyLuigiTask, ABC):
    predict_demand = CoSyLuigiTaskParameter(PredictDemand)
    get_costs = CoSyLuigiTaskParameter(GetCosts)
    output_filename: str = ""

    def _get_cost(self):
        with open(self.input()["get_costs"]["costs"].path, "rb") as f:
            return json.load(f)

    def _get_demand(self):
        demand_df = pd.read_json(self.input()["predict_demand"]["predicted_demand"].path)
        return list(demand_df["predicted_demand"])

    def output(self):
        return {"optimized_lots": luigi.LocalTarget("data/" + self._get_variant_label() + "-" + self.output_filename)}

    def run(self):
        cost = self._get_cost()
        demand = self._get_demand()

        orders = self.run_optimizer(cost, demand)

        with self.output()["optimized_lots"].open("w") as f:
            f.write(str(list(orders)))

    @abstractmethod
    def run_optimizer(self, cost, demand):
        return NotImplementedError()

    def _get_variant_label(self):
        if isinstance(self.input()["predict_demand"]["predicted_demand"], luigi.LocalTarget):
            label = self.input()["predict_demand"]["predicted_demand"].path
            return Path(label).stem
        return None


class OptimizeLotsByGroff(OptimizeLots):
    output_filename = "optimize_lots_by_groff.txt"

    def run_optimizer(self, cost, demand):
        optimizer = GroffHeuristic()
        return optimizer.run(cost, demand)


class OptimizeLotsByWagnerWhitin(OptimizeLots):
    output_filename = "optimize_lots_by_wagner_within.txt"

    def run_optimizer(self, cost, demand):
        optimizer = WagnerWhitin()
        return optimizer.run(cost, demand)


class OptimizeLotsBySilverMeal(OptimizeLots):
    output_filename = "optimize_lots_by_silver_meal.txt"

    def run_optimizer(self, cost, demand):
        optimizer = SilverMeal()
        return optimizer.run(cost, demand)


class OptimizeLotsByLeastUnitCost(OptimizeLots):
    output_filename = "optimize_lots_by_least_unit_cost.txt"

    def run_optimizer(self, cost, demand):
        optimizer = LeastUnitCostMethod()
        return optimizer.run(cost, demand)


class OptimizeLotsByPartPeriod(OptimizeLots):
    output_filename = "optimize_lots_by_part_period.txt"

    def run_optimizer(self, cost, demand):
        optimizer = PartPeriod()
        return optimizer.run(cost, demand)


if __name__ == "__main__":
    repo = CoSyLuigiRepo(
        GetCosts,
        GetHistoricDemand,
        PredictDemandByAverage,
        PredictDemandByLinearRegression,
        OptimizeLotsByLeastUnitCost,
        OptimizeLotsByGroff,
        OptimizeLotsByPartPeriod,
        OptimizeLotsBySilverMeal,
        OptimizeLotsByWagnerWhitin,
    )
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    for result in maestro.query(OptimizeLots.target()):
        # print(deps_tree.print_tree(result))
        luigi.build([result], local_scheduler=True, detailed_summary=True)
