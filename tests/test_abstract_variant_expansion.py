"""Tests if flatten correctly expands abstract classes to their concrete implementing classes for repository
construction. Note that this tests for inheriting from ABC and having abstract methods. This is due to checking for
abstractness in python checks for the presence of abstract methods, but within the modeling context of CoSy-Luigi,
classes can just be identifiers that groud their subclasses, so marking a class abstract by just inheriting from ABC
is a valid use-case."""

from abc import ABC, abstractmethod

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask


class ABCInheritedTaskWithNoInheritors(CoSyLuigiTask, ABC):
    """An abstract class that no other class inherits from. This class is abstract because it inherits from ABC."""


class ABCInheritedTask(CoSyLuigiTask, ABC):
    """An abstract class. This class is abstract because it inherits from ABC."""


class ABCTaskFromABCInherited(ABCInheritedTask, ABC):
    """An abstract class that implements ABCInheritedTask."""


class DeeperConcreteTaskFromABCTaskFromABCInherited(ABCTaskFromABCInherited):
    """A class that indirectly implements ABCInheritedTask by inheriting from ABCTaskFromABCInherited."""


class ConcreteTaskFromABCInherited(ABCInheritedTask):
    """A class that implements ABCInheritedTask."""


class DeeperConcreteTaskFromABCInherited(ConcreteTaskFromABCInherited):
    """A class that indirectly implements ABCInheritedTask by inheriting from ConcreteTaskFromABCInherited."""


# noinspection PyAbstractClass
class AbstractTask(CoSyLuigiTask):
    """An abstract class. This class is abstract because it has an abstract method."""

    @abstractmethod
    def get_class_name(self):
        """Gets the class name. The presence of this method makes the class abstract.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.
        """
        raise NotImplementedError


# noinspection PyAbstractClass
class AbstractTaskWithNoInheritors(CoSyLuigiTask):
    """An abstract class that no other class inherits from. This class is abstract because it has an abstract method."""

    @abstractmethod
    def get_class_name(self):
        """Gets the class name. The presence of this method makes the class abstract.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.
        """
        raise NotImplementedError


class ConcreteTaskFromAbstract(AbstractTask):
    """A class that implements AbstractTask."""

    def get_class_name(self):
        """Overrides get_class_name to concretize the task.

        Returns:
            str: The task's class name.
        """
        return "ConcreteTaskFromAbstract"


class DeeperConcreteTaskFromAbstract(ConcreteTaskFromAbstract):
    """A class that implements AbstractTask by inheriting from ConcreteTaskFromAbstract."""

    def get_class_name(self):
        """Overrides get_class_name to return the correct class name.

        Returns:
            str: The task's class name.
        """
        return "DeeperConcreteTaskFromAbstract"


def test_expansion_from_abc():
    """Tests if adding a class that is abstract because it inherits from ABC expands to all of its subclasses when
    added to a CoSyLuigiRepo."""
    repo = CoSyLuigiRepo(ABCInheritedTask)
    assert repo.luigi_repo == {
        ConcreteTaskFromABCInherited,
        DeeperConcreteTaskFromABCInherited,
        DeeperConcreteTaskFromABCTaskFromABCInherited,
    }


def test_expansion_from_abstract():
    """Tests if adding a class that is abstract because it has abstract methods expands to all of its subclasses when
    added to a CoSyLuigiRepo."""
    repo = CoSyLuigiRepo(AbstractTask)
    assert ConcreteTaskFromAbstract().get_class_name() == "ConcreteTaskFromAbstract"
    assert DeeperConcreteTaskFromAbstract().get_class_name() == "DeeperConcreteTaskFromAbstract"
    assert repo.luigi_repo == {ConcreteTaskFromAbstract, DeeperConcreteTaskFromAbstract}


def test_expansion_from_abc_and_abstract():
    """Tests if adding classes that are abstract because of different reasons expand to all of their subclasses when
    added to CoSyLuigiRepo."""
    repo = CoSyLuigiRepo(ABCInheritedTask, AbstractTask)
    assert repo.luigi_repo == {
        ConcreteTaskFromABCInherited,
        DeeperConcreteTaskFromABCTaskFromABCInherited,
        DeeperConcreteTaskFromABCInherited,
        ConcreteTaskFromAbstract,
        DeeperConcreteTaskFromAbstract,
    }


def test_implementation_of_abstract_does_not_expand():
    """Test if a tasks that concretizes an abstract class does not expand."""
    repo = CoSyLuigiRepo(ConcreteTaskFromAbstract)
    assert repo.luigi_repo == {ConcreteTaskFromAbstract}


def test_implementation_of_abc_does_not_expand():
    """Test if a tasks that it is concrete because it does not directly inherit from ABC does not expand."""
    repo = CoSyLuigiRepo(ConcreteTaskFromABCInherited)
    assert repo.luigi_repo == {ConcreteTaskFromABCInherited}


def test_expansion_to_nothing_from_abc_with_no_inheritors():
    """Tests if an abstract task that is abstract because it inherits from ABC but has no classes that inherit from
    it expands to an empty set."""
    repo = CoSyLuigiRepo(ABCInheritedTaskWithNoInheritors)
    assert repo.luigi_repo == set()


def test_expansion_to_nothing_from_abstract_with_no_inheritors():
    """Tests if an abstract task that is abstract because it has abstract methods but has no classes that inherit
    from it expands to an empty set."""
    repo = CoSyLuigiRepo(AbstractTaskWithNoInheritors)
    assert repo.luigi_repo == set()
