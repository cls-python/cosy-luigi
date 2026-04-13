from abc import ABC, abstractmethod

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask


class ABCInheritedTaskWithNoInheritors(CoSyLuigiTask, ABC):
    pass

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
        raise NotImplementedError


# noinspection PyAbstractClass
class AbstractTaskWithNoInheritors(CoSyLuigiTask):
    @abstractmethod
    def get_class_name(self):
        raise NotImplementedError


class ConcreteTaskFromAbstract(AbstractTask):
    def get_class_name(self):
        return "ConcreteTaskFromAbstract"


class DeeperConcreteTaskFromAbstract(ConcreteTaskFromAbstract):
    def get_class_name(self):
        return "DeeperConcreteTaskFromAbstract"


def test_expansion_from_abc():
    repo = CoSyLuigiRepo(ABCInheritedTask)
    assert repo.luigi_repo == {ConcreteTaskFromABCInherited, DeeperConcreteTaskFromABCInherited}

def test_expansion_from_abstract():
    repo = CoSyLuigiRepo(AbstractTask)
    assert ConcreteTaskFromAbstract().get_class_name() == "ConcreteTaskFromAbstract"
    assert DeeperConcreteTaskFromAbstract().get_class_name() == "DeeperConcreteTaskFromAbstract"
    assert repo.luigi_repo == {ConcreteTaskFromAbstract, DeeperConcreteTaskFromAbstract}

def test_expansion_from_abc_and_abstract():
    repo = CoSyLuigiRepo(ABCInheritedTask, AbstractTask)
    assert repo.luigi_repo == {
        ConcreteTaskFromABCInherited,
        DeeperConcreteTaskFromABCInherited,
        ConcreteTaskFromAbstract,
        DeeperConcreteTaskFromAbstract,
    }

def test_implementation_of_abstract_does_not_expand():
    repo = CoSyLuigiRepo(ConcreteTaskFromAbstract)
    assert repo.luigi_repo == {ConcreteTaskFromAbstract}

def test_implementation_of_abc_does_not_expand():
    repo = CoSyLuigiRepo(ConcreteTaskFromABCInherited)
    assert repo.luigi_repo == {ConcreteTaskFromABCInherited}

def test_expansion_to_nothing_from_abc_with_no_inheritors():
    repo = CoSyLuigiRepo(ABCInheritedTaskWithNoInheritors)
    assert repo.luigi_repo == set()

def test_expansion_to_nothing_from_abstract_with_no_inheritors():
    repo = CoSyLuigiRepo(AbstractTaskWithNoInheritors)
    assert repo.luigi_repo == set()
