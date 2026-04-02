from abc import ABC, abstractmethod

from cosy_luigi.combinatorics import CoSyLuigiRepo, CoSyLuigiTask


class ABCInheritedTask(CoSyLuigiTask, ABC):
    pass


class ConcreteTaskFromABCInherited(ABCInheritedTask):
    pass


class DeeperConcreteTaskFromABCInherited(ConcreteTaskFromABCInherited):
    pass


# noinspection PyAbstractClass
class AbstractTask(CoSyLuigiTask):
    @abstractmethod
    def do_nothing(self):
        pass


class ConcreteTaskFromAbstract(AbstractTask):
    def do_nothing(self):
        pass


class DeeperConcreteTaskFromAbstract(ConcreteTaskFromAbstract):
    def do_nothing(self):
        pass


def test_abstract_variant_expansion():
    repo = CoSyLuigiRepo(ABCInheritedTask)
    assert repo.luigi_repo == {ConcreteTaskFromABCInherited, DeeperConcreteTaskFromABCInherited}
    repo = CoSyLuigiRepo(AbstractTask)
    assert repo.luigi_repo == {ConcreteTaskFromAbstract, DeeperConcreteTaskFromAbstract}
    repo = CoSyLuigiRepo(ABCInheritedTask, AbstractTask)
    assert repo.luigi_repo == {
        ConcreteTaskFromABCInherited,
        DeeperConcreteTaskFromABCInherited,
        ConcreteTaskFromAbstract,
        DeeperConcreteTaskFromAbstract,
    }
    repo = CoSyLuigiRepo(ConcreteTaskFromAbstract)
    assert repo.luigi_repo == {ConcreteTaskFromAbstract}
