"""Tests for the renderer-independent template model: stages, inheritance trees and data flow."""

from abc import ABC

from cosy_luigi import CoSyLuigiRepo, CoSyLuigiTask, CoSyLuigiTaskParameter
from cosy_luigi.visualization.model import analyze, descendants, node_kind, repo_to_info, toposort

from .example_infos import LOT_SIZING, TITLES


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


def test_stage_heads_are_ordered_by_data_flow():
    model = analyze(LOT_SIZING)
    assert model["heads"] == ["GetCosts", "GetHistoricDemand", "PredictDemand", "OptimizeLots"]
    assert model["flow"] == [
        ("GetCosts", "OptimizeLots"),
        ("GetHistoricDemand", "PredictDemand"),
        ("PredictDemand", "OptimizeLots"),
    ]


def test_base_class_is_no_stage():
    model = analyze(LOT_SIZING)
    assert model["base"] == ["CoSyLuigiTask"]
    assert "CoSyLuigiTask" not in model["heads"]
    assert node_kind("CoSyLuigiTask", model) == "base"


def test_phantom_heads_are_reconstructed_from_the_taxonomy():
    """Abstract classes passed to CoSyLuigiRepo are no taxonomy keys, but must still become stage heads."""
    model = analyze(LOT_SIZING)
    assert "PredictDemand" not in LOT_SIZING["taxonomy"]
    assert model["abstract"] == ["OptimizeLots", "PredictDemand"]
    assert model["children"]["PredictDemand"] == ["PredictDemandByAverage", "PredictDemandByLinearRegression"]
    assert node_kind("PredictDemand", model) == "abstract"
    assert node_kind("GetCosts", model) == "concrete"


def test_multi_level_inheritance_uses_the_most_specific_parent():
    model = analyze(TITLES)
    assert model["parent_of"]["SubstituteNameByMsJaneDoeTask"] == "SubstituteByNameAndTitleTask"
    assert model["parent_of"]["SubstituteByNameAndTitleTask"] == "SubstituteNameTask"
    assert sorted(descendants("SubstituteNameTask", model["children"])) == [
        "SubstituteByNameAndTitleTask",
        "SubstituteNameByJohnDoeTask",
        "SubstituteNameByMrJohnDoeTask",
        "SubstituteNameByMsJaneDoeTask",
    ]


def test_node_flow_ends_at_the_declaring_class():
    model = analyze(TITLES)
    assert model["node_flow"] == [
        ("WriteMrTitleTemplateTask", "SubstituteNameByMrJohnDoeTask"),
        ("WriteMsTitleTemplateTask", "SubstituteNameByMsJaneDoeTask"),
        ("WriteTemplateTask", "SubstituteNameTask"),
        ("WriteTitleTemplateTask", "SubstituteByNameAndTitleTask"),
    ]


def test_toposort_keeps_nodes_on_cycles():
    assert toposort(["a", "b", "c"], [("a", "b"), ("b", "a")]) == ["c", "a", "b"]


def test_repo_to_info_keys_requires_by_declaring_class():
    info = repo_to_info(CoSyLuigiRepo(Source, TitleSource, Consumer))
    requires = info["requires"]
    assert requires["Consumer"] == ["Source"]
    assert requires["TitledConsumer"] == ["TitleSource"]
    # overridden declarations are kept in addition to the one of the parent class
    assert requires["MsConsumer"] == ["MsTitleSource"]
    assert requires["MrConsumer"] == ["MrTitleSource"]
    # tasks without parameters of their own get an empty entry, inherited parameters are not repeated
    assert requires["SimpleConsumer"] == []
    assert requires["Source"] == []


def test_repo_to_info_feeds_analyze():
    model = analyze(repo_to_info(CoSyLuigiRepo(Source, TitleSource, Consumer)))
    assert model["heads"] == ["Source", "TitleSource", "Consumer"]
    assert ("MsTitleSource", "MsConsumer") in model["node_flow"]
    assert ("TitleSource", "TitledConsumer") in model["node_flow"]
