from abc import ABC, abstractmethod

from cosy_luigi.combinatorics import CoSyLuigiRepo, CoSyLuigiTask


class ABCInheritedTask(CoSyLuigiTask, ABC):
    pass


class ConcreteTaskFromABCInherited(ABCInheritedTask):
    pass


# noinspection PyAbstractClass
class AbstractTask(CoSyLuigiTask):
    @abstractmethod
    def do_nothing(self):
        pass


class ConcreteTaskFromAbstract(AbstractTask):
    def do_nothing(self):
        pass


def test_abstract_variant_expansion():
    repo = CoSyLuigiRepo(ABCInheritedTask)
    assert repo.luigi_repo == [ConcreteTaskFromABCInherited]
    repo = CoSyLuigiRepo(AbstractTask)
    assert repo.luigi_repo == [ConcreteTaskFromAbstract]
    repo = CoSyLuigiRepo(ABCInheritedTask, AbstractTask)
    assert repo.luigi_repo == [ConcreteTaskFromABCInherited, ConcreteTaskFromAbstract]
