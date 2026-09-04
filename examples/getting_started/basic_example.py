from maestro import LocalTarget, Maestro, Repository, Task, TaskParameter, run_pipelines


class TaskA(Task):
    def output(self):
        return {"a_artifact": LocalTarget("output/task_a_output.txt")}

    def run(self):
        with self.output()["a_artifact"].open("w") as f:
            f.write("Task A completed")


class TaskB(Task):
    task_a = TaskParameter(TaskA)

    def output(self):
        return {"b_artifact": LocalTarget("output/task_b_output.txt")}

    def run(self):
        with (
            self.input()["task_a"]["a_artifact"].open() as input_file,
            self.output()["b_artifact"].open("w") as output_file,
        ):
            data = input_file.read()
            output_file.write("Task B completed with input: " + data)


if __name__ == "__main__":
    repo = Repository(
        TaskA,
        TaskB,
    )
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    results = maestro.query(TaskB.target())
    run_pipelines(results).report()
