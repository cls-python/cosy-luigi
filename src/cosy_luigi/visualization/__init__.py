"""Visualization of pipeline templates: which tasks a repository contains, how they inherit from each
other and how data flows between them.

    from cosy_luigi.visualization import visualize_template
    visualize_template(repo, "output/pipeline")  # pipeline.html (interactive), pipeline.png/.svg (Graphviz)

In Jupyter, ``PipelineView(repo)`` shows the interactive view directly in the notebook.

These convenience functions take a ``CoSyLuigiRepo``. For a ``{"taxonomy": ..., "requires": ...}`` dict
(see ``repo_to_info``) use ``PipelineView.from_info``, ``render_html`` or ``graphviz.render``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cosy_luigi.visualization import graphviz, interactive
from cosy_luigi.visualization.interactive import PipelineView, render_html, to_html
from cosy_luigi.visualization.model import analyze, repo_to_info, require_repo

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from cosy_luigi import CoSyLuigiRepo

__all__ = ["PipelineView", "analyze", "render_html", "repo_to_info", "to_html", "visualize_template"]


def visualize_template(
    repo: CoSyLuigiRepo,
    basename: str | Path = "template_pipeline",
    formats: Iterable[str] = ("html", "png", "svg"),
    *,
    inline_elk: bool = False,
) -> list[Path]:
    """Render the template of a repository and return the written files.

    Args:
        repo: the repository whose template is rendered
        basename: path without extension, e.g. ``"output/pipeline"``
        formats: ``"html"`` (interactive) and any Graphviz output format such as ``"png"``, ``"svg"``,
            ``"pdf"``; for Graphviz formats the ``.dot`` source is written as well
        inline_elk: embed elkjs into the HTML so that it also works offline
    """
    info = repo_to_info(require_repo(repo, "render_html(info, basename) or graphviz.render(info, basename)"))
    formats = list(formats)
    written = []
    if "html" in formats:
        written.append(interactive.render_html(info, basename, inline_elk=inline_elk))
    image_formats = [f for f in formats if f != "html"]
    if image_formats:
        written += graphviz.render(info, basename, formats=image_formats)
    return written
