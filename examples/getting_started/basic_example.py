import textwrap

import luigi
from cosy.maestro import Maestro

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter


class TaskA(CoSyLuigiTask):
    def output(self):
        return {"a_artifact": luigi.LocalTarget("output/task_a_output.txt")}

    def run(self):
        with self.output()["a_artifact"].open("w") as f:
            f.write("Task A completed")


class TaskB(CoSyLuigiTask):
    task_a = CoSyLuigiTaskParameter(TaskA)

    def output(self):
        return {"b_artifact": luigi.LocalTarget("output/task_b_output.txt")}

    def run(self):
        with (
            self.input()["task_a"]["a_artifact"].open() as input_file,
            self.output()["b_artifact"].open("w") as output_file,
        ):
            data = input_file.read()
            output_file.write("Task B completed with input: " + data)


if __name__ == "__main__":
    repo = CoSyLuigiRepo(
        TaskA,
        TaskB,
    )
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    results = list(maestro.query(TaskB.target()))
    luigi.build(results, local_scheduler=True, detailed_summary=True)
    print(
        textwrap.dedent(
            f"""
                ===============================================
                    There are a total of {len(results)} results
                ==============================================="""
        )
    )
