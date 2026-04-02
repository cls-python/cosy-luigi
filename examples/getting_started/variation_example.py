import textwrap
from abc import ABC
from string import Template

import luigi
from cosy.maestro import Maestro

from src.cosy_luigi.combinatorics import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter


class WriteTemplateTask(CoSyLuigiTask):
    def output(self):
        return {"template": luigi.LocalTarget("hello_world_template.txt")}

    def run(self):
        with self.output()["template"].open("w") as result:
            result.write("Hello World $name")


class SubstituteNameTask(CoSyLuigiTask, ABC):
    template_task = CoSyLuigiTaskParameter(WriteTemplateTask)
    name: str = None

    def output(self):
        return {"filled_template": luigi.LocalTarget(self.__class__.__name__ + "_filled_template.txt")}

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
    repo = CoSyLuigiRepo(WriteTemplateTask, SubstituteNameTask)
    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    results = list(maestro.query(SubstituteNameTask.target()))
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
