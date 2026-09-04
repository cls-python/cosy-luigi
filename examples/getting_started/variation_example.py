from abc import ABC
from string import Template

from maestro import LocalTarget, Maestro, Repository, Task, TaskParameter, run_pipelines


class WriteTemplateTask(Task):
    def output(self):
        return {"template": LocalTarget("hello_world_template.txt")}

    def run(self):
        with self.output()["template"].open("w") as result:
            result.write("Hello World $name")


class SubstituteNameTask(Task, ABC):
    template_task = TaskParameter(WriteTemplateTask)
    name: str = None

    def output(self):
        return {"filled_template": LocalTarget(self.__class__.__name__ + "_filled_template.txt")}

    def run(self):
        with self.input()["template_task"]["template"].open() as input_template:
            template = Template(input_template.read())
            result = template.substitute(name=self.name)
            with self.output()["filled_template"].open("w") as outfile:
                outfile.write(result)


class SubstituteNameByJohnDoeTask(SubstituteNameTask):
    name = "John Doe"


class SubstituteNameByJaneDoeTask(SubstituteNameTask):
    name = "Jane Doe"


def main():
    repo = Repository(WriteTemplateTask, SubstituteNameTask)
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    results = maestro.query(SubstituteNameTask.target())
    run_pipelines(results).report()


if __name__ == "__main__":
    main()
