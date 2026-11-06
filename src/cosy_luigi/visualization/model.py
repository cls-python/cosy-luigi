"""Renderer-independent model of a pipeline template.

From a ``{"taxonomy": ..., "requires": ...}`` dict (see :func:`repo_to_info`) it derives the pipeline
stages (heads), the inheritance trees below them and the data flow between classes. Both the Graphviz
renderer and the interactive HTML view are built on top of :func:`analyze`.
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from cosy_luigi import CoSyLuigiRepo


def analyze(info: Mapping[str, Any]) -> dict[str, Any]:
    """Derive stages, inheritance trees and data flow from a template info dict.

    Returns a dict with the keys ``concrete``, ``abstract``, ``base``, ``parent_of``, ``children``,
    ``heads`` (stage heads in topological order of the data flow), ``flow`` (data flow aggregated to
    stages) and ``node_flow`` (data flow from the required class to the declaring class).
    """
    tax = {c: set(ps) for c, ps in info["taxonomy"].items()}
    req = info["requires"]

    concrete = list(tax)
    ancestors = sorted({p for ps in tax.values() for p in ps})

    # The base class is an ancestor of every concrete task (e.g. CoSyLuigiTask) and no pipeline stage.
    base = {a for a in ancestors if all(a in tax[c] for c in concrete)}

    # Not only the keys of the taxonomy are nodes: abstract classes passed directly to CoSyLuigiRepo(...)
    # are resolved into their concrete variants by flatten() and therefore never appear as a key, only as
    # an ancestor of other classes ("phantom" nodes). They still have to be drawn as stage heads.
    all_nodes = (set(concrete) | set(ancestors)) - base

    # How many concrete tasks have n as a (transitive) ancestor? The smaller this coverage, the more
    # specific n is. This also works for phantom nodes without an entry of their own.
    def coverage(n: str) -> int:
        return sum(1 for c in concrete if n in tax.get(c, set()))

    # Ancestors of a node. For phantom nodes (possibly nested over several levels) they are reconstructed
    # as the intersection of the ancestors of all concrete tasks that have n as an ancestor.
    def node_ancestors(n: str) -> set[str]:
        if n in tax:
            return tax[n]
        covering = [tax[c] for c in concrete if n in tax[c]]
        return set.intersection(*covering) - {n} if covering else set()

    # The direct parent is the most specific ancestor (smallest coverage), so for A -> B(ABC) -> C the
    # parent of C is B, not A.
    parent_of: dict[str, str | None] = {}
    for c in sorted(all_nodes):
        specific = [p for p in node_ancestors(c) if p not in base]
        parent_of[c] = min(specific, key=lambda p: (coverage(p), p)) if specific else None

    children: defaultdict[str, list[str]] = defaultdict(list)
    for c in sorted(all_nodes):
        parent = parent_of[c]
        if parent is not None:
            children[parent].append(c)

    # Stage heads are the roots of the inheritance chains; abstract means having a subclass in the repo.
    heads = [c for c in sorted(all_nodes) if parent_of[c] is None]
    abstract = sorted(children)

    def stage_of(n: str) -> str:
        while (parent := parent_of.get(n)) is not None:
            n = parent
        return n

    # Data flow aggregated to stages; it determines the left-to-right order of the stage heads.
    flow = {(stage_of(n), stage_of(task)) for task, needs in req.items() for n in needs}
    flow = {(a, b) for a, b in flow if a != b}

    # Data flow between nodes: from the required class to the class declaring the parameter, exactly as
    # written in the source code (a stage head, an intermediate class or a concrete class).
    node_flow = {(n, task) for task, needs in req.items() for n in needs if n != task}

    return {
        "concrete": concrete,
        "abstract": abstract,
        "base": sorted(base),
        "parent_of": parent_of,
        "children": children,
        "heads": toposort(heads, flow),
        "flow": sorted(flow),
        "node_flow": sorted(node_flow),
    }


def toposort(nodes: list[str], edges: Iterable[tuple[str, str]]) -> list[str]:
    """Order nodes so that edges point left to right; stable and robust against cycles."""
    indeg = dict.fromkeys(nodes, 0)
    adj: defaultdict[str, list[str]] = defaultdict(list)
    for a, b in edges:
        if a in indeg and b in indeg:
            adj[a].append(b)
            indeg[b] += 1
    queue = deque(sorted(n for n in nodes if indeg[n] == 0))
    out = []
    while queue:
        n = queue.popleft()
        out.append(n)
        for m in sorted(adj[n]):
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    return out + [n for n in nodes if n not in out]  # nodes on cycles keep their input order


def descendants(n: str, children: Mapping[str, list[str]]) -> list[str]:
    """All descendants of n over any number of inheritance levels."""
    stack, seen = list(children.get(n, [])), []
    while stack:
        cur = stack.pop()
        seen.append(cur)
        stack.extend(children.get(cur, []))
    return seen


def node_kind(n: str, model: Mapping[str, Any]) -> str:
    """Return ``"base"``, ``"abstract"`` or ``"concrete"``; each renderer maps this to its own colors."""
    if n in model["base"]:
        return "base"
    if n in model["abstract"]:
        return "abstract"
    return "concrete"


def require_repo(source: object, alternative: str) -> CoSyLuigiRepo:
    """Return ``source`` if it is a ``CoSyLuigiRepo``, otherwise raise a ``TypeError`` naming the dict-based
    ``alternative``. Keeps the convenience functions strict instead of guessing the type of their input."""
    from cosy_luigi import CoSyLuigiRepo  # noqa: PLC0415  (avoid import cycle)

    if not isinstance(source, CoSyLuigiRepo):
        msg = (
            f"expected a CoSyLuigiRepo, got {type(source).__name__}; "
            f"for a {{'taxonomy': ..., 'requires': ...}} dict use {alternative}"
        )
        raise TypeError(msg)
    return source


def repo_to_info(repo: CoSyLuigiRepo) -> dict[str, Any]:
    """Build the ``{"taxonomy": ..., "requires": ...}`` dict for :func:`analyze` from a repository.

    ``requires`` is keyed by the class whose body declares a ``CoSyLuigiTaskParameter``, not by every
    concrete task that inherits it. Overridden declarations are kept, so every place in the source code
    shows up in the visualization. Example::

        {
            "taxonomy": {"MsConsumer": {"CoSyLuigiTask", "Consumer", "TitledConsumer"}, ...},
            "requires": {"Consumer": ["Source"], "TitledConsumer": ["TitleSource"], "MsConsumer": ["MsTitleSource"]},
        }

    The taxonomy maps every concrete task to all of its ancestors (as ``repo.taxonomy`` does); abstract
    classes passed to the repository only appear as ancestors.
    """
    from cosy_luigi import CoSyLuigiTask, CoSyLuigiTaskParameter  # noqa: PLC0415  (avoid import cycle)

    taxonomy = dict(repo.taxonomy)
    nodes = set(taxonomy) | {p for ps in taxonomy.values() for p in ps}

    requires: defaultdict[str, set[str]] = defaultdict(set)
    for task in repo.luigi_repo:
        requires[task.__name__]
        for cls in task.__mro__:
            if not (isinstance(cls, type) and issubclass(cls, CoSyLuigiTask)) or cls is CoSyLuigiTask:
                continue
            # A declaration in a class outside the repository (e.g. a mixin) is attributed to the task.
            owner = cls.__name__ if cls.__name__ in nodes else task.__name__
            for attr in vars(cls).values():
                if isinstance(attr, CoSyLuigiTaskParameter):
                    requires[owner].add(attr.required_task.__name__)

    return {"taxonomy": taxonomy, "requires": {c: sorted(needs) for c, needs in requires.items()}}
