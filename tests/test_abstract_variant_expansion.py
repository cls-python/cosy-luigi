from abc import ABC, abstractmethod

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask


class ABCInheritedTask(CoSyLuigiTask, ABC):
    pass


class ConcreteTaskFromABCInherited(ABCInheritedTask):
    pass


class DeeperConcreteTaskFromABCInherited(ConcreteTaskFromABCInherited):
    pass


# noinspection PyAbstractClass
class AbstractTask(CoSyLuigiTask):
    @abstractmethod
    def get_class_name(self):
        pass


class ConcreteTaskFromAbstract(AbstractTask):
    def get_class_name(self):
        return "ConcreteTaskFromAbstract"


class DeeperConcreteTaskFromAbstract(ConcreteTaskFromAbstract):
    def get_class_name(self):
        return "DeeperConcreteTaskFromAbstract"


def test_abstract_variant_expansion():
    repo = CoSyLuigiRepo(ABCInheritedTask)
    assert repo.luigi_repo == {ConcreteTaskFromABCInherited, DeeperConcreteTaskFromABCInherited}
    repo = CoSyLuigiRepo(AbstractTask)
    assert ConcreteTaskFromAbstract().get_class_name() == "ConcreteTaskFromAbstract"
    assert DeeperConcreteTaskFromAbstract().get_class_name() == "DeeperConcreteTaskFromAbstract"
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
