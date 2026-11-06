import textwrap
from abc import ABC
from string import Template

import luigi
from cosy.maestro import Maestro

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter
from cosy_luigi.visualization import visualize_template


class WriteTemplateTask(CoSyLuigiTask):
    def output(self):
        return {"template": luigi.LocalTarget("output/substitute_name_template.txt")}

    def run(self):
        with self.output()["template"].open("w") as result:
            result.write("Name: $name")


class WriteTitleTemplateTask(CoSyLuigiTask, ABC):
    title: str = None

    def output(self):
        return {"template": luigi.LocalTarget("output/" + self.__class__.__name__ + "_template.txt")}

    def run(self):
        with self.output()["template"].open("w") as result:
            result.write("Name: " + self.title + " $name")


class WriteMsTitleTemplateTask(WriteTitleTemplateTask):
    title = "Ms."


class WriteMrTitleTemplateTask(WriteTitleTemplateTask):
    title = "Mr."


class SubstituteNameTask(CoSyLuigiTask, ABC):
    template_task = CoSyLuigiTaskParameter(WriteTemplateTask)
    name: str = None

    def output(self):
        return {"filled_template": luigi.LocalTarget("output/" + self.__class__.__name__ + "_filled_template.txt")}

    def run(self):
        with self.input()["template_task"]["template"].open() as input_template:
            template = Template(input_template.read())
            result = template.substitute(name=self.name)
            with self.output()["filled_template"].open("w") as outfile:
                outfile.write(result)


class SubstituteNameByJohnDoeTask(SubstituteNameTask):
    name = "John Doe"


class SubstituteByNameAndTitleTask(SubstituteNameTask, ABC):
    title_template_task = CoSyLuigiTaskParameter(WriteTitleTemplateTask)

    def run(self):
        with self.input()["title_template_task"]["template"].open() as input_template:
            template = Template(input_template.read())
            result = template.substitute(name=self.name)
            with self.output()["filled_template"].open("w") as outfile:
                outfile.write(result)


class SubstituteNameByMsJaneDoeTask(SubstituteByNameAndTitleTask):
    title_template_task = CoSyLuigiTaskParameter(WriteMsTitleTemplateTask)
    name = "Jane Doe"


class SubstituteNameByMrJohnDoeTask(SubstituteByNameAndTitleTask):
    title_template_task = CoSyLuigiTaskParameter(WriteMrTitleTemplateTask)
    name = "John Doe"


def main():
    repo = CoSyLuigiRepo(
        WriteTemplateTask,
        WriteTitleTemplateTask,
        SubstituteNameTask,
    )

    # output/visualization_example.html (interactive) and .png/.svg (Graphviz)
    visualize_template(repo, "output/visualization_example")

    maestro = Maestro(repo.cls_repo, repo.taxonomy)
    results = maestro.query(SubstituteNameTask.target())
    luigi.build(results, local_scheduler=True, detailed_summary=True)
    print(
        textwrap.dedent(
            f"""
                ===============================================
                    There are a total of {len(list(results))} results
                ==============================================="""
        )
    )
    # results.visualize()


if __name__ == "__main__":
    main()
