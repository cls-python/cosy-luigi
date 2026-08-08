#!/usr/bin/env python3
"""
Baut aus einem {'taxonomy': ..., 'requires': ...}-Dict ein Graphviz-Diagramm:
  - waagerechte Reihe von Pipeline-Stufen (abstrakte Koepfe)
  - konkrete Implementierungen NEBENEINANDER eine Ebene darunter
  - gestrichelte Vererbungspfeile von unten in die Elternklasse
  - durchgezogene Datenfluss-Pfeile gemaess 'requires'
"""
from collections import defaultdict, deque
import subprocess, shutil, os



STYLE = dict(
    abstract_fill="#cfe2f3", abstract_border="#3d6fb4",
    concrete_fill="#d9ead3", concrete_border="#6aa84f",
    base_fill="#e8e8e8",     base_border="#888888",
    font="Helvetica", fontsize=12,
    nodesep=0.30, ranksep=0.60,
    show_base=False,     # gemeinsame Basisklasse (z.B. CoSyLuigiTask) zeichnen?
    ortho=False,         # rechtwinklige Kanten statt Kurven
)


# ------------------------------------------------------------ Modellanalyse
def analyze(info, show_base=True):
    tax, req = info['taxonomy'], info['requires']

    concrete = list(tax.keys())
    ancestors = sorted({p for ps in tax.values() for p in ps})

    # Basisklasse = Elternteil (nahezu) aller konkreten Tasks -> keine Pipeline-Stufe
    base = {a for a in ancestors if sum(a in tax[c] for c in concrete) == len(concrete)}

    # Nicht nur tax.keys() sind Knoten: abstrakte Klassen, die direkt an CoSyLuigiRepo(...)
    # uebergeben werden, werden von flatten() in ihre konkreten Varianten aufgeloest und
    # tauchen deshalb NIE als eigener Schluessel in tax auf ("Phantom"-Knoten) - nur als
    # Vorfahre in den tax-Werten anderer Klassen. Damit sie trotzdem als Pipeline-Stufen-Kopf
    # gezeichnet werden, behandeln wir hier alle referenzierten Namen als Knoten.
    all_nodes = (set(concrete) | set(ancestors)) - base

    # Wie viele konkrete Tasks im Repo haben n (transitiv) als Vorfahren? Je kleiner diese
    # Abdeckung, desto spezifischer/naeher am Blatt ist n. Das funktioniert auch fuer
    # Phantom-Knoten, die selbst keinen eigenen Eintrag in tax haben (siehe oben) - wir
    # muessen dafuer nicht wissen, WEN n beerbt, nur WESSEN Vorfahre n selbst ist.
    def coverage(n):
        return sum(1 for c in concrete if n in tax.get(c, set()))

    # Vorfahren-Menge eines Knotens. Fuer echte Repo-Mitglieder direkt aus tax bekannt. Fuer
    # Phantom-Knoten (auch mehrstufig verschachtelt, z.B. wenn sowohl Elternteil als auch
    # Grosselternteil selbst nie Schluessel in tax sind) gibt es keinen eigenen Eintrag - wir
    # rekonstruieren ihn aus dem Schnitt der Vorfahren-Mengen ALLER konkreten Tasks, die n als
    # Vorfahren fuehren: was die alle gemeinsam haben, muss n selbst geerbt haben.
    def node_ancestors(n):
        if n in tax:
            return tax[n]
        covering = [tax[c] for c in concrete if n in tax[c]]
        return set.intersection(*covering) - {n} if covering else set()

    # direkte Elternklasse eines Tasks = spezifischster Vorfahre (ohne Basisklasse).
    # node_ancestors(c) ist nur eine ungeordnete Menge ALLER Vorfahren (beliebige Tiefe), daher
    # waehlen wir davon denjenigen mit der kleinsten Abdeckung - der ist am weitesten "unten" in
    # der Kette, also der naechste/spezifischste Vorfahre. Bei mehrstufiger Abstraktion
    # (z.B. A -> B(ABC) -> C) landet so B als direkter Elternteil von C, nicht A.
    parent_of = {}
    for c in sorted(all_nodes):
        specific = [p for p in node_ancestors(c) if p not in base]
        parent_of[c] = min(specific, key=lambda p: (coverage(p), p)) if specific else None

    # Kinder je Elternklasse - rekursiv ueber beliebig viele Ebenen, nicht nur eine
    children = defaultdict(list)
    for c in sorted(all_nodes):
        if parent_of[c] is not None:
            children[parent_of[c]].append(c)

    # Stufen-Koepfe: Klassen ohne Elternteil im Repo (Wurzeln je Vererbungskette)
    heads = [c for c in sorted(all_nodes) if parent_of[c] is None]

    # abstrakt = hat mindestens eine Unterklasse im Repo (unabhaengig von der Tiefe)
    abstract = sorted(children.keys())

    # Datenfluss auf Stufen-Ebene aggregieren: requires eines Tasks -> Kante von der
    # benoetigten Stufe zur eigenen Stufe. stage_of laeuft dazu die Elternkette bis zur
    # Wurzel (dem Kopf der Pipeline-Stufe) hoch, unabhaengig davon wie viele Zwischen-
    # Abstraktionsebenen dazwischen liegen.
    def stage_of(n):
        while parent_of.get(n) is not None:
            n = parent_of[n]
        return n

    flow = set()
    for task, needs in req.items():
        dst = stage_of(task)
        for n in needs:
            src = stage_of(n)
            if src != dst:
                flow.add((src, dst))

    order = toposort(heads, flow)
    return dict(concrete=concrete, abstract=abstract, base=sorted(base),
                parent_of=parent_of, heads=order, children=children,
                flow=sorted(flow), stage_of=stage_of)


def toposort(nodes, edges):
    """Stufen von links nach rechts; stabil, zyklusfest."""
    indeg = {n: 0 for n in nodes}
    adj = defaultdict(list)
    for a, b in edges:
        if a in indeg and b in indeg:
            adj[a].append(b); indeg[b] += 1
    q = deque(sorted(n for n in nodes if indeg[n] == 0))
    out = []
    while q:
        n = q.popleft(); out.append(n)
        for m in sorted(adj[n]):
            indeg[m] -= 1
            if indeg[m] == 0: q.append(m)
    out += [n for n in nodes if n not in out]      # Rest bei Zyklen
    return out


# ----------------------------------------------------------------- DOT-Bau
def descendants(n, children):
    """Sammelt rekursiv alle Nachkommen von n ueber beliebig viele Vererbungsebenen (nicht nur die direkten
    Kinder), damit z.B. ein Cluster pro Pipeline-Stufe die komplette Unterklassen-Hierarchie umfasst."""
    stack, seen = list(children.get(n, [])), []
    while stack:
        cur = stack.pop()
        seen.append(cur)
        stack.extend(children.get(cur, []))
    return seen


def build_dot(model, style=STYLE):
    S = style
    A, B = set(model['abstract']), set(model['base'])

    def colors(n):
        if n in B: return S['base_fill'], S['base_border']
        if n in A: return S['abstract_fill'], S['abstract_border']
        return S['concrete_fill'], S['concrete_border']

    def node(n, indent="  "):
        f, b = colors(n)
        return f'{indent}"{n}" [fillcolor="{f}", color="{b}"];'

    L = ["digraph Pipeline {",
         "  rankdir=TB; newrank=true; compound=true;",
         f"  nodesep={S['nodesep']}; ranksep={S['ranksep']};",
         "  splines=ortho;" if S['ortho'] else "  splines=spline;",
         f'  node [shape=box, style="filled,rounded", fontname="{S["font"]}", '
         f'fontsize={S["fontsize"]}, margin="0.16,0.07"];',
         f'  edge [fontname="{S["font"]}", fontsize=10];']

    # --- Knoten je Stufe in einem unsichtbaren Cluster - Kopf plus ALLE Nachkommen,
    # damit auch mehrstufige Abstraktion (Kopf -> Zwischenklasse -> konkrete Klasse) in
    # derselben Spalte gruppiert bleibt statt ueber die ganze Breite verteilt zu werden.
    for i, h in enumerate(model['heads']):
        L.append(f"  subgraph cluster_{i} {{ style=invis;")
        L.append(node(h, "    "))
        for c in descendants(h, model['children']):
            L.append(node(c, "    "))
        L.append("  }")

    # --- optionale gemeinsame Basisklasse
    if S['show_base'] and model['base']:
        for b in model['base']:
            L.append(node(b))

    # --- obere Reihe: alle Stufen-Koepfe auf gleichem Rang
    L.append("  { rank=same; " + "; ".join(f'"{h}"' for h in model['heads']) + "; }")

    # --- Vererbung: JEDES Eltern-Kind-Paar (nicht nur Kopf -> direktes Kind), damit
    # mehrstufige Abstraktion korrekt dargestellt wird. dir=back => Pfeilspitze oben an
    # der Elternklasse. Die Ranks tieferer Ebenen ergeben sich automatisch aus den Kanten.
    L.append(f'  edge [style=dashed, color="#555555", penwidth=1.0, arrowsize=0.8, dir=back];')
    for parent, kids in model['children'].items():
        for c in kids:
            L.append(f'  "{parent}" -> "{c}";')

    # --- Basisklasse: alle konkreten Tasks erben davon (dezent, ohne Layout-Einfluss)
    if S['show_base'] and model['base']:
        L.append('  edge [style=dotted, color="#999999", penwidth=0.8, arrowsize=0.6, '
                 'dir=back, constraint=false];')
        for b in model['base']:
            for h in model['heads']:
                L.append(f'  "{b}" -> "{h}";')

    # --- Datenfluss zwischen den Stufen (waagerecht, ohne Rangeinfluss)
    L.append('  edge [style=solid, color="#333333", penwidth=1.2, arrowsize=0.8, '
             'dir=forward, constraint=false, weight=5];')
    for a, b in model['flow']:
        L.append(f'  "{a}" -> "{b}";')

    # --- Reihenfolge der Koepfe links->rechts erzwingen
    L.append('  edge [style=invis, constraint=false, weight=50];')
    L.append("  " + " -> ".join(f'"{h}"' for h in model['heads']) + ";")

    L.append("}")
    return "\n".join(L)


# --------------------------------------------------------- Matplotlib-Fallback
def render_mpl(info, basename="taxonomy_pipeline", style=STYLE, formats=("png",)):
    """Rendert dasselbe Diagramm wie render(), aber ohne Graphviz - nur mit matplotlib."""
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyArrowPatch

    out_dir = os.path.dirname(basename)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    S = style
    model = analyze(info, S['show_base'])
    heads, children, base, abstract = model['heads'], model['children'], set(model['base']), set(model['abstract'])

    def colors(n):
        if n in base: return S['base_fill'], S['base_border']
        if n in abstract: return S['abstract_fill'], S['abstract_border']
        return S['concrete_fill'], S['concrete_border']

    # Layout: eine Spalte pro Kopf, Kinder nebeneinander darunter zentriert.
    # slot_gap wird an die laengste Beschriftung angepasst, damit sich die Boxen nicht ueberlappen.
    inches_per_unit = 0.5
    all_names = list(heads) + [c for h in heads for c in children[h]]
    max_len = max(len(n) for n in all_names)
    fontsize = 9 if max_len > 25 else S['fontsize']
    label_inches = max_len * fontsize / 72 * 0.6
    slot_gap, col_gap = max(1.4, label_inches / inches_per_unit + 0.6), 1.0
    pos, x_cursor = {}, 0.0
    for h in heads:
        kids = children[h]
        slot = max(len(kids), 1) * slot_gap
        pos[h] = (x_cursor + slot / 2, 1.6)
        for i, c in enumerate(kids):
            pos[c] = (x_cursor + slot_gap / 2 + i * slot_gap, 0.0)
        x_cursor += slot + col_gap

    fig, ax = plt.subplots(figsize=(x_cursor * inches_per_unit, 4))
    ax.set_xlim(-0.5, x_cursor)
    ax.set_ylim(-0.6, 2.2)
    ax.axis("off")

    def draw_node(n):
        fc, ec = colors(n)
        x, y = pos[n]
        ax.text(x, y, n, ha="center", va="center", fontsize=fontsize, family="sans-serif", zorder=3,
                 bbox=dict(boxstyle="round,pad=0.35", fc=fc, ec=ec, lw=1.4))

    for h in heads:
        draw_node(h)
        for c in children[h]:
            draw_node(c)

    # Naeherungsweise Box-Halbmasse (Textlaenge -> Breite, feste Zeilenhoehe) fuer Kantenanker an den Raendern
    half_h = 0.16

    def half_w(n):
        return max(0.3, len(n) * fontsize / 72 * 0.6 / inches_per_unit / 2 + 0.04)

    # Vererbung: senkrecht - von der Oberkante des Kindes zur Unterkante des Kopfes (Pfeilspitze oben, analog dir=back)
    for h in heads:
        for c in children[h]:
            cx, cy = pos[c]
            hx, hy = pos[h]
            start = (cx, cy + half_h)
            end = (hx, hy - half_h)
            ax.add_patch(FancyArrowPatch(start, end, arrowstyle="-|>", linestyle="dashed",
                                          color="#555555", mutation_scale=12, lw=1.0, zorder=1))

    # Datenfluss zwischen Stufen: waagerecht - von der rechten/linken Kante je nach Richtung, leicht gebogen
    for a, b in model['flow']:
        ax_, ay_ = pos[a]
        bx_, by_ = pos[b]
        if ax_ <= bx_:
            start = (ax_ + half_w(a), ay_)
            end = (bx_ - half_w(b), by_)
        else:
            start = (ax_ - half_w(a), ay_)
            end = (bx_ + half_w(b), by_)
        ax.add_patch(FancyArrowPatch(start, end, arrowstyle="-|>", connectionstyle="arc3,rad=0.25",
                                      color="#333333", mutation_scale=14, lw=1.3, zorder=2))

    fig.tight_layout()
    for fmt in formats:
        fig.savefig(f"{basename}.{fmt}", dpi=150)
    plt.close(fig)
    return model


# -------------------------------------------------------------------- Main
def render(info, basename="template_pipeline", style=STYLE, formats=("png", "svg")):
    out_dir = os.path.dirname(basename)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    model = analyze(info, style['show_base'])
    dot = build_dot(model, style)
    with open(f"{basename}.dot", "w") as fh:
        fh.write(dot)
    if shutil.which("dot"):
        for fmt in formats:
            args = ["dot", f"-T{fmt}", f"{basename}.dot", "-o", f"{basename}.{fmt}"]
            if fmt == "png": args[1:1] = ["-Gdpi=150"]
            subprocess.run(args, check=True)
    else:
        render_mpl(info, basename, style, formats=formats)
    return model, dot


def render_repo_template(repo, basename="template_pipeline"):
    info = {
        "taxonomy": dict(repo.taxonomy),
        "requires": {
            task.__name__: [param.required_task.__name__ for _, param in task.get_params()] for task in repo.luigi_repo
        },
    }
    print(info)

    render(info, basename)

if __name__ == "__main__":

    print("======================== start visu")
    info = {
        'taxonomy': {
            'PredictDemandByLinearRegression': ['CoSyLuigiTask', 'PredictDemand'],
            'GetCosts': ['CoSyLuigiTask'],
            'GetHistoricDemand': ['CoSyLuigiTask'],
            'OptimizeLotsByLeastUnitCost': ['CoSyLuigiTask', 'OptimizeLots'],
            'OptimizeLotsBySilverMeal': ['CoSyLuigiTask', 'OptimizeLots'],
            'PredictDemandByAverage': ['CoSyLuigiTask', 'PredictDemand'],
            'OptimizeLotsByGroff': ['CoSyLuigiTask', 'OptimizeLots'],
            'OptimizeLotsByWagnerWhitin': ['CoSyLuigiTask', 'OptimizeLots'],
            'OptimizeLotsByPartPeriod': ['CoSyLuigiTask', 'OptimizeLots'],
        },
        'requires': {
            'PredictDemandByLinearRegression': ['GetHistoricDemand'],
            'GetCosts': [],
            'GetHistoricDemand': [],
            'OptimizeLotsByLeastUnitCost': ['PredictDemand', 'GetCosts'],
            'OptimizeLotsBySilverMeal': ['PredictDemand', 'GetCosts'],
            'PredictDemandByAverage': ['GetHistoricDemand'],
            'OptimizeLotsByGroff': ['PredictDemand', 'GetCosts'],
            'OptimizeLotsByWagnerWhitin': ['PredictDemand', 'GetCosts'],
            'OptimizeLotsByPartPeriod': ['PredictDemand', 'GetCosts'],
        },
    }

    model, dot = render(info)
    print("Stufen (links->rechts):", " -> ".join(model['heads']))
    print("Basisklasse:", model['base'])
    for h in model['heads']:
        if model['children'][h]:
            print(f"  {h}: {model['children'][h]}")
    print("Datenfluss:", model['flow'])