"""Static rendering of a pipeline template with Graphviz (``.dot``, ``.png``, ``.svg``).

  - stage heads in one row, ordered by data flow from left to right
  - subclasses below their parent, grouped per stage
  - dashed inheritance arrows pointing to the parent class
  - solid data flow arrows from the required class to the class declaring the parameter

Rendering images requires the Graphviz ``dot`` executable; without it only the ``.dot`` file is written.
"""

from __future__ import annotations

import shutil
import subprocess
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosy_luigi.visualization.model import analyze, descendants, node_kind

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

STYLE: dict[str, Any] = {
    "abstract_fill": "#cfe2f3",
    "abstract_border": "#3d6fb4",
    "concrete_fill": "#d9ead3",
    "concrete_border": "#6aa84f",
    "base_fill": "#e8e8e8",
    "base_border": "#888888",
    "font": "Helvetica",
    "fontsize": 12,
    "nodesep": 0.30,
    "ranksep": 0.60,
    "show_base": False,  # draw the common base class (e.g. CoSyLuigiTask)?
    "ortho": False,  # right-angled edges instead of curves
}


def build_dot(model: Mapping[str, Any], style: Mapping[str, Any] = STYLE) -> str:
    """Build the Graphviz source for a model returned by :func:`~cosy_luigi.visualization.model.analyze`."""
    s = style

    def node(n: str, indent: str = "  ") -> str:
        kind = node_kind(n, model)
        return f'{indent}"{n}" [fillcolor="{s[f"{kind}_fill"]}", color="{s[f"{kind}_border"]}"];'

    lines = [
        "digraph Pipeline {",
        "  rankdir=TB; newrank=true; compound=true;",
        f"  nodesep={s['nodesep']}; ranksep={s['ranksep']};",
        "  splines=ortho;" if s["ortho"] else "  splines=spline;",
        f'  node [shape=box, style="filled,rounded", fontname="{s["font"]}", '
        f'fontsize={s["fontsize"]}, margin="0.16,0.07"];',
        f'  edge [fontname="{s["font"]}", fontsize=10];',
    ]

    # One invisible cluster per stage (head plus ALL descendants), so that multi-level abstraction
    # (head -> intermediate class -> concrete class) stays in the same column.
    for i, h in enumerate(model["heads"]):
        lines.append(f"  subgraph cluster_{i} {{ style=invis;")
        lines.append(node(h, "    "))
        lines.extend(node(c, "    ") for c in descendants(h, model["children"]))
        lines.append("  }")

    if s["show_base"]:
        lines.extend(node(b) for b in model["base"])

    # All stage heads on the same rank
    lines.append("  { rank=same; " + "; ".join(f'"{h}"' for h in model["heads"]) + "; }")

    # Inheritance for EVERY parent-child pair; dir=back puts the arrowhead at the parent class.
    lines.append('  edge [style=dashed, color="#555555", penwidth=1.0, arrowsize=0.8, dir=back];')
    lines.extend(f'  "{parent}" -> "{c}";' for parent, kids in model["children"].items() for c in kids)

    # Base class: every stage head inherits from it (subtle, without influence on the layout)
    if s["show_base"] and model["base"]:
        lines.append('  edge [style=dotted, color="#999999", penwidth=0.8, arrowsize=0.6, dir=back, constraint=false];')
        lines.extend(f'  "{b}" -> "{h}";' for b in model["base"] for h in model["heads"])

    # Data flow from the required class to the declaring class (without influence on the ranks)
    lines.append(
        '  edge [style=solid, color="#333333", penwidth=1.2, arrowsize=0.8, dir=forward, constraint=false, weight=5];'
    )
    lines.extend(f'  "{a}" -> "{b}";' for a, b in model["node_flow"])

    # Enforce the left-to-right order of the stage heads
    lines.append("  edge [style=invis, constraint=false, weight=50];")
    lines.append("  " + " -> ".join(f'"{h}"' for h in model["heads"]) + ";")

    lines.append("}")
    return "\n".join(lines)


def render(
    info: Mapping[str, Any],
    basename: str | Path = "template_pipeline",
    style: Mapping[str, Any] = STYLE,
    formats: Iterable[str] = ("png", "svg"),
) -> list[Path]:
    """Write ``{basename}.dot`` and render it to the given formats; returns the written files."""
    dot_path = Path(f"{basename}.dot")
    dot_path.parent.mkdir(parents=True, exist_ok=True)
    dot_path.write_text(build_dot(analyze(info), style), encoding="utf-8")
    written = [dot_path]

    formats = [f for f in formats if f != "dot"]  # the source itself is always written
    dot = shutil.which("dot")
    if not formats:
        return written
    if dot is None:
        warnings.warn(f"Graphviz ('dot') not found on PATH, only {dot_path} was written.", stacklevel=2)
        return written
    for fmt in formats:
        out = dot_path.with_suffix(f".{fmt}")
        dpi = ["-Gdpi=150"] if fmt == "png" else []
        subprocess.run([dot, *dpi, f"-T{fmt}", str(dot_path), "-o", str(out)], check=True)
        written.append(out)
    return written
