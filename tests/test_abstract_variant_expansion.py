"""_summary_."""
from abc import ABC, abstractmethod

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask


class ABCInheritedTaskWithNoInheritors(CoSyLuigiTask, ABC):
    """_summary_."""
    pass


class ABCInheritedTask(CoSyLuigiTask, ABC):
    """_summary_."""
    pass


class ConcreteTaskFromABCInherited(ABCInheritedTask):
    """_summary_."""
    pass


class DeeperConcreteTaskFromABCInherited(ConcreteTaskFromABCInherited):
    """_summary_."""
    pass


# noinspection PyAbstractClass
class AbstractTask(CoSyLuigiTask):
    """_summary_."""
    @abstractmethod
    def get_class_name(self):
        """_summary_.

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError


# noinspection PyAbstractClass
class AbstractTaskWithNoInheritors(CoSyLuigiTask):
    """_summary_."""
    @abstractmethod
    def get_class_name(self):
        """_summary_.

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError


class ConcreteTaskFromAbstract(AbstractTask):
    """_summary_."""
    def get_class_name(self):
        """_summary_.

        Returns:
            _type_: _description_
        """
        return "ConcreteTaskFromAbstract"


class DeeperConcreteTaskFromAbstract(ConcreteTaskFromAbstract):
    """_summary_."""
    def get_class_name(self):
        """_summary_.

        Returns:
            _type_: _description_
        """
        return "DeeperConcreteTaskFromAbstract"


def test_expansion_from_abc():
    """_summary_."""
    repo = CoSyLuigiRepo(ABCInheritedTask)
    assert repo.luigi_repo == {ConcreteTaskFromABCInherited, DeeperConcreteTaskFromABCInherited}


def test_expansion_from_abstract():
    """_summary_."""
    repo = CoSyLuigiRepo(AbstractTask)
    assert ConcreteTaskFromAbstract().get_class_name() == "ConcreteTaskFromAbstract"
    assert DeeperConcreteTaskFromAbstract().get_class_name() == "DeeperConcreteTaskFromAbstract"
    assert repo.luigi_repo == {ConcreteTaskFromAbstract, DeeperConcreteTaskFromAbstract}


def test_expansion_from_abc_and_abstract():
    """_summary_."""
    repo = CoSyLuigiRepo(ABCInheritedTask, AbstractTask)
    assert repo.luigi_repo == {
        ConcreteTaskFromABCInherited,
        DeeperConcreteTaskFromABCInherited,
        ConcreteTaskFromAbstract,
        DeeperConcreteTaskFromAbstract,
    }


def test_implementation_of_abstract_does_not_expand():
    """_summary_."""
    repo = CoSyLuigiRepo(ConcreteTaskFromAbstract)
    assert repo.luigi_repo == {ConcreteTaskFromAbstract}


def test_implementation_of_abc_does_not_expand():
    """_summary_."""
    repo = CoSyLuigiRepo(ConcreteTaskFromABCInherited)
    assert repo.luigi_repo == {ConcreteTaskFromABCInherited}


def test_expansion_to_nothing_from_abc_with_no_inheritors():
    """_summary_."""
    repo = CoSyLuigiRepo(ABCInheritedTaskWithNoInheritors)
    assert repo.luigi_repo == set()


def test_expansion_to_nothing_from_abstract_with_no_inheritors():
    """_summary_."""
    repo = CoSyLuigiRepo(AbstractTaskWithNoInheritors)
    assert repo.luigi_repo == set()
