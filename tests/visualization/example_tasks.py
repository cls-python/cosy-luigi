"""Task classes of a small repository used by the visualization tests.

Covers a parameter declared on a stage head, on an abstract intermediate class (on an abstract stage head)
and overridden by concrete classes (on concrete variants of another stage).
"""

from abc import ABC

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter


class Source(CoSyLuigiTask):
    """A concrete stage head without parameters."""


class TitleSource(CoSyLuigiTask, ABC):
    """An abstract stage head with two concrete variants."""


class MsTitleSource(TitleSource):
    """A concrete variant of TitleSource."""


class MrTitleSource(TitleSource):
    """A concrete variant of TitleSource."""


class Consumer(CoSyLuigiTask, ABC):
    """An abstract stage head declaring a parameter."""

    source = CoSyLuigiTaskParameter(Source)


class SimpleConsumer(Consumer):
    """A concrete class without parameters of its own."""


class TitledConsumer(Consumer, ABC):
    """An abstract intermediate class declaring a parameter on an abstract stage head."""

    title_source = CoSyLuigiTaskParameter(TitleSource)


class MsConsumer(TitledConsumer):
    """Overrides the parameter of its parent with a concrete variant."""

    title_source = CoSyLuigiTaskParameter(MsTitleSource)


class MrConsumer(TitledConsumer):
    """Overrides the parameter of its parent with a concrete variant."""

    title_source = CoSyLuigiTaskParameter(MrTitleSource)


def make_repo() -> CoSyLuigiRepo:
    return CoSyLuigiRepo(Source, TitleSource, Consumer)
