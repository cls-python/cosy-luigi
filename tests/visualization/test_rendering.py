"""Tests for the Graphviz source and the interactive HTML page."""

import re
import warnings

import pytest

from cosy_luigi.visualization import PipelineView, render_html, to_html, visualize_template
from cosy_luigi.visualization.graphviz import build_dot
from cosy_luigi.visualization.interactive import ELK_URL, _script, build_view_model
from cosy_luigi.visualization.model import analyze

from .example_infos import LOT_SIZING, TITLES
from .example_tasks import make_repo


def test_dot_puts_heads_in_one_row_in_data_flow_order():
    dot = build_dot(analyze(LOT_SIZING))
    assert '{ rank=same; "GetCosts"; "GetHistoricDemand"; "PredictDemand"; "OptimizeLots"; }' in dot
    assert '"GetCosts" -> "GetHistoricDemand" -> "PredictDemand" -> "OptimizeLots";' in dot


def test_dot_draws_data_flow_to_the_declaring_class():
    dot = build_dot(analyze(TITLES))
    assert '"WriteTitleTemplateTask" -> "SubstituteByNameAndTitleTask";' in dot
    assert '"WriteMsTitleTemplateTask" -> "SubstituteNameByMsJaneDoeTask";' in dot


def test_view_model_lists_each_stage_head_before_its_subclasses():
    nodes = [n["id"] for n in build_view_model(analyze(LOT_SIZING))["nodes"]]
    assert nodes.index("PredictDemand") < nodes.index("PredictDemandByAverage") < nodes.index("OptimizeLots")


def test_embedded_scripts_cannot_end_their_script_block():
    assert "</script>" not in _script('const s = "</script>";')[len("<script>") : -len("</script>")]


def test_html_page_is_self_contained_except_for_elkjs():
    page = to_html(TITLES, title="<Titles>")
    assert "<title>&lt;Titles&gt;</title>" in page
    assert f'<script src="{ELK_URL}"></script>' in page
    assert 'customElements.define("pipeline-view"' in page
    assert '"SubstituteNameByMsJaneDoeTask"' in page
    # no other external resources
    assert re.findall(r'src="(https?://[^"]+)"', page) == [ELK_URL]


def test_inline_elk_embeds_the_cached_library(tmp_path, monkeypatch):
    monkeypatch.setenv("COSY_LUIGI_CACHE", str(tmp_path))
    (tmp_path / "elk-0.9.3.bundled.js").write_text("var FAKE_ELK = 1;", encoding="utf-8")
    page = to_html(TITLES, inline_elk=True)
    assert "var FAKE_ELK = 1;" in page
    assert ELK_URL not in page


def test_render_html_writes_the_page(tmp_path):
    path = render_html(TITLES, tmp_path / "out" / "titles")
    assert path == tmp_path / "out" / "titles.html"
    assert "<title>titles</title>" in path.read_text(encoding="utf-8")


def test_pipeline_view_embeds_the_page_as_iframe():
    view = PipelineView.from_info(TITLES, height=300)
    snippet = view._repr_html_()
    assert snippet.startswith('<iframe srcdoc="')
    assert "height: 300px" in snippet
    assert "<script" not in snippet  # the page is escaped inside the attribute


def test_pipeline_view_takes_a_repository():
    assert '"MsConsumer"' in PipelineView(make_repo()).html


def test_visualize_template_writes_requested_formats(tmp_path):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # Graphviz may be missing; the .dot source is written anyway
        written = visualize_template(make_repo(), tmp_path / "repo", formats=("html", "dot"))
    names = {p.name for p in written}
    assert {"repo.html", "repo.dot"} <= names


def test_convenience_functions_only_take_a_repository(tmp_path):
    with pytest.raises(TypeError, match=r"PipelineView\.from_info"):
        PipelineView(TITLES)
    with pytest.raises(TypeError, match="render_html"):
        visualize_template(TITLES, tmp_path / "titles")
