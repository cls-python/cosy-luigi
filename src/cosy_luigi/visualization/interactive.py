"""Interactive rendering of a pipeline template as a self-contained HTML page.

  - ELK (layered, top-down) places the nodes based on inheritance: stage heads in one row, parents
    centered above their subclasses; inheritance edges leave the parent at the bottom and enter the
    child at the top
  - data flow edges do not influence the layout (like ``constraint=false`` in Graphviz); a custom
    orthogonal router (``assets/grid_router.js``) routes them around the boxes
  - nodes can be dragged (together with their subclasses) and snap to a grid; pan and zoom

The view is the web component ``<pipeline-view>`` (``assets/pipeline_view.*``). The page template,
component and router are embedded into the HTML when rendering, so the output stays a single file
(ES modules would be blocked under ``file://``).

Usage:
  - standalone HTML (e.g. for a review):  ``render_html(info, "out/pipeline")`` -> ``out/pipeline.html``
  - Jupyter:  ``PipelineView(repo_or_info, height=500)`` as the last expression of a cell
  - documentation (MkDocs, Sphinx, ...):  embed the generated file with
    ``<iframe src="pipeline.html" width="100%" height="500"></iframe>``
  - as a string:  ``to_html(info)``

With ``inline_elk=True`` elkjs (about 1.5 MB) is embedded instead of loaded from a CDN, so the file also
works offline (e.g. for archived review files). elkjs is downloaded once and kept in the user cache.
"""

from __future__ import annotations

import html
import json
import os
import urllib.request
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosy_luigi.visualization.model import analyze, descendants, node_kind, repo_to_info

if TYPE_CHECKING:
    from collections.abc import Mapping

STYLE: dict[str, Any] = {
    "colors": {
        "abstract": {"fill": "#cfe2f3", "border": "#3d6fb4"},
        "concrete": {"fill": "#d9ead3", "border": "#6aa84f"},
        "base": {"fill": "#e8e8e8", "border": "#888888"},
    },
    "font": "Helvetica, Arial, sans-serif",
    "fontsize": 13,
    "padding_x": 14,
    "padding_y": 8,
    "corner_radius": 8,
    "node_spacing": 40,  # gap between neighboring nodes
    "layer_spacing": 64,  # gap between inheritance levels
    "inheritance": {"color": "#555555", "width": 1.2, "dash": "5,4"},
    "flow": {"color": "#333333", "width": 1.5},
    "highlight": "#e69138",
    "grid": {"size": 20, "dot_radius": 1, "color": "#c4c4cc"},  # dot grid in the background
}

ELK_URL = "https://cdn.jsdelivr.net/npm/elkjs@0.9.3/lib/elk.bundled.js"

_ASSETS = files("cosy_luigi.visualization").joinpath("assets")
_COMPONENT = "pipeline_view.html"  # <template> with markup and CSS of the component
_SCRIPTS = ("grid_router.js", "pipeline_view.js")  # order matters: the component uses GridRouter
_PLACEHOLDER = "<!--__SCRIPTS__-->"


def build_view_model(model: Mapping[str, Any], style: Mapping[str, Any] = STYLE) -> dict[str, Any]:
    """Input of the ``<pipeline-view>`` component.

    The node order matters: ELK keeps it (considerModelOrder), so the stage heads stay in topological
    order and every inheritance tree stays below its head.
    """
    nodes = [
        {"id": n, "kind": node_kind(n, model), "stage": h}
        for h in model["heads"]
        for n in [h, *descendants(h, model["children"])]
    ]
    # Draw required classes outside the repository as well, otherwise their edges would dangle
    known = {n["id"] for n in nodes}
    for edge in model["node_flow"]:
        for n in edge:
            if n not in known:
                nodes.append({"id": n, "kind": node_kind(n, model), "stage": n})
                known.add(n)

    return {
        "nodes": nodes,
        "inheritance": [[p, c] for p, kids in model["children"].items() for c in kids],
        "flow": [list(e) for e in model["node_flow"]],
        "style": style,
    }


def _script(js: str) -> str:
    # Escape "</": a "</script>" inside the embedded code (class name, comment, string) would otherwise end
    # the script block. Inside JS strings and comments "<\/" means the same as "</".
    return "<script>\n" + js.replace("</", "<\\/") + "\n</script>"


def _elk_source() -> str:
    """elkjs for ``inline_elk=True``: downloaded from the CDN once, then read from the user cache."""
    cache_dir = Path(os.environ.get("COSY_LUIGI_CACHE", Path.home() / ".cache" / "cosy_luigi"))
    cache = cache_dir / "elk-0.9.3.bundled.js"
    if not cache.is_file():
        cache.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(ELK_URL, timeout=60) as response:  # noqa: S310  (fixed https URL)
            cache.write_bytes(response.read())
    return cache.read_text(encoding="utf-8")


def to_html(
    info: Mapping[str, Any],
    style: Mapping[str, Any] = STYLE,
    title: str = "Pipeline template",
    *,
    inline_elk: bool = False,
) -> str:
    """Return a complete, self-contained HTML page with the interactive view."""
    view_model = build_view_model(analyze(info), style)
    scripts = ["const MODEL = " + json.dumps(view_model, indent=1) + ";"]
    scripts += [_ASSETS.joinpath(name).read_text(encoding="utf-8") for name in _SCRIPTS]
    elk = _script(_elk_source()) if inline_elk else f'<script src="{ELK_URL}"></script>'
    component = _ASSETS.joinpath(_COMPONENT).read_text(encoding="utf-8")
    page = _ASSETS.joinpath("template.html").read_text(encoding="utf-8")
    page = page.replace("__TITLE__", html.escape(title)).replace("<!--__ELK__-->", elk)
    return page.replace(_PLACEHOLDER, component + "\n".join(_script(js) for js in scripts))


def render_html(
    info: Mapping[str, Any],
    basename: str | Path = "template_pipeline",
    style: Mapping[str, Any] = STYLE,
    title: str | None = None,
    *,
    inline_elk: bool = False,
) -> Path:
    """Write ``{basename}.html`` and return its path."""
    path = Path(f"{basename}.html")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(to_html(info, style, title or path.stem, inline_elk=inline_elk), encoding="utf-8")
    return path


class PipelineView:
    """Interactive view for Jupyter (JupyterLab, Notebook, VS Code).

    Show it as the last expression of a cell. It is embedded as ``<iframe srcdoc=...>``, so its scripts
    also run in untrusted notebooks and several views in one notebook do not interfere.

    Args:
        source: a ``CoSyLuigiRepo`` or a ``{"taxonomy": ..., "requires": ...}`` dict
        height: height in the notebook (pixels or a CSS length)
    """

    def __init__(
        self,
        source: Any,
        height: float | str = 500,
        style: Mapping[str, Any] = STYLE,
        title: str = "Pipeline template",
        *,
        inline_elk: bool = False,
    ) -> None:
        info = repo_to_info(source) if hasattr(source, "taxonomy") else source
        self.html = to_html(info, style, title, inline_elk=inline_elk)
        self.height = f"{height}px" if isinstance(height, (int, float)) else height

    def _repr_html_(self) -> str:
        return (
            f'<iframe srcdoc="{html.escape(self.html, quote=True)}" '
            f'style="width: 100%; height: {self.height}; border: 1px solid #e5e5e5; border-radius: 4px;">'
            "</iframe>"
        )

    def save(self, path: str | Path) -> Path:
        """Save the same page as a standalone HTML file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.html, encoding="utf-8")
        return path
