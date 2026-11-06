// Web component <pipeline-view>: interactive visualization of a pipeline template.
//   ELK places the nodes (stage heads in one row, inheritance trees centered below them),
//   GridRouter (grid_router.js) routes the data flow edges around the boxes.
//
// Usage:
//   <pipeline-view></pipeline-view>
//   <script> document.querySelector("pipeline-view").model = MODEL; </script>
//
// Requires in the document: the <template id="pipeline-view-template"> (pipeline_view.html) and the
// globals ELK (elkjs) and GridRouter. Several instances per page are possible; each has its own shadow
// root, CSS and state.
(() => {
  const SVG_NS = "http://www.w3.org/2000/svg";
  const measureCtx = document.createElement("canvas").getContext("2d");

  function el(tag, attrs = {}, parent = null) {
    const e = document.createElementNS(SVG_NS, tag);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
    if (parent) parent.appendChild(e);
    return e;
  }

  // Polyline with slightly rounded corners
  function roundedPath(pts, r = 6) {
    let d = `M${pts[0]}`;
    for (let i = 1; i < pts.length - 1; i++) {
      const [p, c, n] = [pts[i - 1], pts[i], pts[i + 1]];
      const l1 = Math.hypot(c[0] - p[0], c[1] - p[1]), l2 = Math.hypot(n[0] - c[0], n[1] - c[1]);
      const k = Math.min(r, l1 / 2, l2 / 2);
      if (!k) { d += ` L${c}`; continue; }
      const a = [c[0] + (p[0] - c[0]) * k / l1, c[1] + (p[1] - c[1]) * k / l1];
      const b = [c[0] + (n[0] - c[0]) * k / l2, c[1] + (n[1] - c[1]) * k / l2];
      d += ` L${a} Q${c} ${b}`;
    }
    return d + ` L${pts[pts.length - 1]}`;
  }

  class PipelineView extends HTMLElement {
    constructor() {
      super();
      const root = this.attachShadow({ mode: "open" });
      root.appendChild(document.getElementById("pipeline-view-template").content.cloneNode(true));
      const $ = sel => root.querySelector(sel);
      this.$ = { svg: $(".canvas"), viewport: $(".viewport"), edges: $(".edges"), nodes: $(".nodes"),
                 defs: $(".defs"), legend: $(".legend"), error: $(".error"),
                 gridPattern: $(".grid-pattern"), gridDot: $(".grid-dot"), root };
      this.view = { k: 1, tx: 0, ty: 0 };
      this.snap = true;           // snap dragged nodes to the grid (Shift inverts)
      this.drag = null;
      this.pan = null;
      this._redrawPending = false;
      this._bindUi();
    }

    // ---------------------------------------------------------------- public interface
    set model(m) { this._model = m; if (this.isConnected) this._init(); }
    get model() { return this._model; }

    connectedCallback() { if (this._model && !this._started) this._init(); }

    async reset() { await this._runLayout(); this._redraw(); this.fit(); }

    // Zoom around a point of the canvas (pixels relative to it); around the center if omitted
    zoomBy(factor, mx, my) {
      const r = this.$.svg.getBoundingClientRect(), v = this.view;
      if (mx === undefined) { mx = r.width / 2; my = r.height / 2; }
      const k = Math.min(4, Math.max(0.1, v.k * factor));
      v.tx = mx - (mx - v.tx) * k / v.k;
      v.ty = my - (my - v.ty) * k / v.k;
      v.k = k;
      this._applyView();
    }

    fit() {
      const bb = this.$.viewport.getBBox(), r = this.$.svg.getBoundingClientRect();
      if (!bb.width || !bb.height || !r.width || !r.height) return;
      // larger margin left and top, so the control bar and the legend do not cover nodes
      const pad = 24, padLeft = 56, padTop = 56, v = this.view;
      v.k = Math.min((r.width - pad - padLeft) / bb.width, (r.height - pad - padTop) / bb.height, 1.5);
      v.tx = padLeft + (r.width - pad - padLeft - bb.width * v.k) / 2 - bb.x * v.k;
      v.ty = padTop + (r.height - pad - padTop - bb.height * v.k) / 2 - bb.y * v.k;
      this._applyView();
    }

    exportSvg() {
      this._highlight(null);
      const bb = this.$.viewport.getBBox(), pad = 20;
      const clone = this.$.svg.cloneNode(true);
      clone.removeAttribute("class");
      clone.setAttribute("viewBox", `${bb.x - pad} ${bb.y - pad} ${bb.width + 2 * pad} ${bb.height + 2 * pad}`);
      clone.setAttribute("width", bb.width + 2 * pad); clone.setAttribute("height", bb.height + 2 * pad);
      clone.querySelector(".viewport").removeAttribute("transform");
      clone.querySelector(".grid").remove();      // the grid is not part of the export
      clone.querySelectorAll("title").forEach(t => t.remove());
      const blob = new Blob(['<?xml version="1.0" encoding="UTF-8"?>\n' + new XMLSerializer().serializeToString(clone)],
                            { type: "image/svg+xml" });
      const a = Object.assign(document.createElement("a"),
                              { href: URL.createObjectURL(blob), download: (document.title || "pipeline") + ".svg" });
      a.click(); URL.revokeObjectURL(a.href);
    }

    // ---------------------------------------------------------------- setup
    async _init() {
      this._started = true;
      const M = this._model, S = this.S = M.style;
      this._buildLegend();
      const g = S.grid, gp = this.$.gridPattern;
      gp.setAttribute("width", g.size); gp.setAttribute("height", g.size);
      this.$.gridDot.setAttribute("r", g.dot_radius); this.$.gridDot.setAttribute("fill", g.color);
      if (typeof ELK === "undefined") {
        this.$.error.style.display = "block";
        this.$.error.textContent = "elkjs could not be loaded (no internet connection?). " +
                                   "For offline use, create the HTML with inline_elk=True.";
        return;
      }
      this.$.defs.replaceChildren();
      this._marker("arrow-inh", S.inheritance.color);
      this._marker("arrow-flow", S.flow.color);
      this._marker("arrow-hl", S.highlight);

      const textWidth = (text, italic) => {
        measureCtx.font = `${italic ? "italic " : ""}${S.fontsize}px ${S.font}`;
        return measureCtx.measureText(text).width;
      };
      this.nodes = M.nodes.map(n => ({
        ...n, w: Math.ceil(textWidth(n.id, n.kind === "abstract")) + 2 * S.padding_x, h: S.fontsize + 2 * S.padding_y + 4,
      }));
      this.byId = Object.fromEntries(this.nodes.map(n => [n.id, n]));
      this.childrenOf = {};
      for (const [p, c] of M.inheritance) (this.childrenOf[p] ||= []).push(c);
      this.edges = [
        ...M.inheritance.map(([p, c]) => ({ type: "inh", src: p, dst: c, key: `inh:${p}->${c}` })),
        ...M.flow.map(([a, b]) => ({ type: "flow", src: a, dst: b, key: `flow:${a}->${b}` })),
      ];
      this.flowEdges = this.edges.filter(e => e.type === "flow");

      this._buildScene();
      await this._runLayout();
      this._redraw();
      this.fit();
      this.dataset.ready = "1";
      this.dispatchEvent(new CustomEvent("ready"));
    }

    // Arrowheads: one per color (context-stroke is not supported everywhere)
    _marker(id, color) {
      const m = el("marker", { id, viewBox: "0 0 10 10", refX: 9, refY: 5, markerWidth: 10, markerHeight: 10,
                               orient: "auto", markerUnits: "userSpaceOnUse" }, this.$.defs);
      el("path", { d: "M0,0 L10,5 L0,10 z", fill: color }, m);
    }

    _descendants(id) {
      const out = [], stack = [...(this.childrenOf[id] || [])];
      while (stack.length) { const c = stack.pop(); out.push(c); stack.push(...(this.childrenOf[c] || [])); }
      return out;
    }

    // ---------------------------------------------------------------- Layout (ELK)
    // Only inheritance determines the layout (data flow does not, like constraint=false in Graphviz).
    // The node order of the model is kept: stage heads in topological order, every inheritance tree
    // below its head. Ports: bottom of the parent class, top of the child class.
    async _runLayout() {
      const S = this.S, inheritance = this._model.inheritance;
      const graph = {
        id: "root",
        layoutOptions: {
          "elk.algorithm": "layered",
          "elk.direction": "DOWN",
          "elk.edgeRouting": "ORTHOGONAL",
          "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES",
          "elk.layered.crossingMinimization.forceNodeModelOrder": "true",
          "elk.separateConnectedComponents": "false",
          // parents centered above their children (otherwise ELK aligns them with a single child)
          "elk.layered.nodePlacement.bk.fixedAlignment": "BALANCED",
          "elk.spacing.nodeNode": String(S.node_spacing),
          "elk.layered.spacing.nodeNodeBetweenLayers": String(S.layer_spacing),
          "elk.layered.spacing.edgeNodeBetweenLayers": String(S.layer_spacing / 3),
          "elk.padding": "[top=40,left=40,bottom=40,right=40]",
        },
        children: this.nodes.map(n => ({
          id: n.id, width: n.w, height: n.h,
          layoutOptions: { "elk.portConstraints": "FIXED_SIDE" },
          ports: [
            { id: `${n.id}|S`, width: 0, height: 0, layoutOptions: { "elk.port.side": "SOUTH" } },
            { id: `${n.id}|N`, width: 0, height: 0, layoutOptions: { "elk.port.side": "NORTH" } },
          ],
        })),
        edges: inheritance.map(([p, c], i) => ({ id: `e${i}`, sources: [`${p}|S`], targets: [`${c}|N`] })),
      };
      const result = await new ELK().layout(graph);
      this.pos = {};
      for (const c of result.children) this.pos[c.id] = { x: c.x, y: c.y };
      this.elkRoutes = {};        // inheritance edges: points from ELK, valid until an end node is moved
      result.edges.forEach((r, i) => {
        const [p, c] = inheritance[i];
        const pts = [];
        for (const s of r.sections || []) pts.push(s.startPoint, ...(s.bendPoints || []), s.endPoint);
        if (pts.length) this.elkRoutes[`inh:${p}->${c}`] = pts.map(q => [q.x, q.y]);
      });
    }

    // ---------------------------------------------------------------- edge geometry
    _box(id) { return { id, ...this.pos[id], w: this.byId[id].w, h: this.byId[id].h }; }

    // Inheritance at moved nodes: bottom center -> bend halfway -> top center
    _inheritanceRoute(e) {
      if (this.elkRoutes[e.key]) return this.elkRoutes[e.key];
      const a = this._box(e.src), b = this._box(e.dst);
      const p = [a.x + a.w / 2, a.y + a.h], c = [b.x + b.w / 2, b.y], midY = (p[1] + c[1]) / 2;
      return [p, [p[0], midY], [c[0], midY], c];
    }

    // Data flow: preferably out on the right and in on the left; top/bottom only if needed
    _routeFlows() {
      this.flowRoutes = GridRouter.route(
        this.nodes.map(n => this._box(n.id)),
        this.flowEdges.map(e => ({ key: e.key, src: e.src, dst: e.dst, srcSides: ["E", "N", "S"], dstSides: ["W", "N", "S"] })),
      );
    }

    _edgePath(e) {
      // draw inheritance from child to parent, so the arrowhead (marker-end) is at the parent
      if (e.type === "inh") return roundedPath(this._inheritanceRoute(e).slice().reverse());
      return roundedPath(this.flowRoutes[e.key]);
    }

    // ---------------------------------------------------------------- drawing
    _buildScene() {
      const S = this.S;
      this.$.edges.replaceChildren(); this.$.nodes.replaceChildren();
      this.edgeEls = {}; this.nodeEls = {};
      for (const e of this.edges) {
        const st = e.type === "inh" ? S.inheritance : S.flow;
        const attrs = { fill: "none", stroke: st.color, "stroke-width": st.width,
                        "marker-end": `url(#arrow-${e.type})`, class: `edge ${e.type}` };
        if (e.type === "inh") attrs["stroke-dasharray"] = st.dash;
        this.edgeEls[e.key] = el("path", attrs, this.$.edges);
      }
      for (const n of this.nodes) {
        const c = S.colors[n.kind] || S.colors.concrete;
        const g = el("g", { class: `node ${n.kind}` }, this.$.nodes);
        el("rect", { width: n.w, height: n.h, rx: S.corner_radius, fill: c.fill, stroke: c.border, "stroke-width": 1.5 }, g);
        const tx = el("text", { x: n.w / 2, y: n.h / 2 + 1, "text-anchor": "middle", "dominant-baseline": "central",
                                "font-family": S.font, "font-size": S.fontsize, fill: "#222" }, g);
        if (n.kind === "abstract") tx.setAttribute("font-style", "italic");
        tx.textContent = n.id;
        const title = el("title", {}, g); title.textContent = `${n.id} (${n.kind}, stage ${n.stage})`;
        g.addEventListener("pointerdown", ev => this._startDrag(ev, n.id));
        g.addEventListener("pointerenter", () => this._highlight(n.id));
        g.addEventListener("pointerleave", () => this._highlight(null));
        this.nodeEls[n.id] = g;
      }
    }

    _redraw() {
      for (const n of this.nodes) this.nodeEls[n.id].setAttribute("transform", `translate(${this.pos[n.id].x},${this.pos[n.id].y})`);
      this._routeFlows();
      for (const e of this.edges) this.edgeEls[e.key].setAttribute("d", this._edgePath(e));
    }

    // While dragging, route at most once per frame
    _requestRedraw() {
      if (this._redrawPending) return;
      this._redrawPending = true;
      requestAnimationFrame(() => { this._redrawPending = false; this._redraw(); });
    }

    _highlight(id) {
      if (this.drag) return;
      const S = this.S, connected = new Set(id ? [id] : []);
      for (const e of this.edges) {
        const on = id && (e.src === id || e.dst === id);
        const p = this.edgeEls[e.key], st = e.type === "inh" ? S.inheritance : S.flow;
        p.setAttribute("stroke", on ? S.highlight : st.color);
        p.setAttribute("marker-end", `url(#arrow-${on ? "hl" : e.type})`);
        p.classList.toggle("dim", !!id && !on);
        if (on) { connected.add(e.src); connected.add(e.dst); this.$.edges.appendChild(p); }
      }
      for (const n of this.nodes) this.nodeEls[n.id].classList.toggle("dim", !!id && !connected.has(n.id));
    }

    _buildLegend() {
      const S = this._model.style, lg = this.$.legend;
      lg.replaceChildren();
      const item = html => { const s = document.createElement("span"); s.innerHTML = html; lg.appendChild(s); };
      for (const [kind, label] of [["abstract", "abstract"], ["concrete", "concrete"]]) {
        const c = S.colors[kind];
        item(`<i class="box" style="background:${c.fill};border-color:${c.border}"></i>${label}`);
      }
      item(`<svg width="28" height="10"><line x1="0" y1="5" x2="28" y2="5" stroke="${S.inheritance.color}"
            stroke-width="${S.inheritance.width}" stroke-dasharray="${S.inheritance.dash}"/></svg>Inheritance`);
      item(`<svg width="28" height="10"><line x1="0" y1="5" x2="28" y2="5" stroke="${S.flow.color}"
            stroke-width="${S.flow.width}"/></svg>Data flow`);
    }

    // ---------------------------------------------------------------- view and interaction
    _applyView() {
      const v = this.view, t = `translate(${v.tx},${v.ty}) scale(${v.k})`;
      this.$.viewport.setAttribute("transform", t);
      this.$.gridPattern.setAttribute("patternTransform", t);
    }

    _toGraph(ev) {
      const r = this.$.svg.getBoundingClientRect(), v = this.view;
      return [(ev.clientX - r.left - v.tx) / v.k, (ev.clientY - r.top - v.ty) / v.k];
    }

    _startDrag(ev, id) {
      ev.stopPropagation();
      const ids = ev.altKey ? [id] : [id, ...this._descendants(id)];
      this.drag = { start: this._toGraph(ev), ids, from: Object.fromEntries(ids.map(i => [i, { ...this.pos[i] }])) };
      // discard ELK routes of inheritance edges at moved nodes -> simple fallback route
      const moved = new Set(ids);
      for (const e of this.edges) if (e.type === "inh" && (moved.has(e.src) || moved.has(e.dst))) delete this.elkRoutes[e.key];
      this.$.svg.setPointerCapture(ev.pointerId);
    }

    _bindUi() {
      const svg = this.$.svg, root = this.$.root;

      svg.addEventListener("wheel", ev => {
        ev.preventDefault();
        const r = svg.getBoundingClientRect();
        this.zoomBy(Math.exp(-ev.deltaY * 0.0015), ev.clientX - r.left, ev.clientY - r.top);
      }, { passive: false });

      svg.addEventListener("pointerdown", ev => {
        if (ev.target.closest(".node")) return;
        this.pan = { x: ev.clientX, y: ev.clientY, tx: this.view.tx, ty: this.view.ty };
        svg.setPointerCapture(ev.pointerId); svg.classList.add("panning");
      });
      svg.addEventListener("pointermove", ev => {
        if (this.drag) {
          const [x, y] = this._toGraph(ev), d = this.drag;
          let dx = x - d.start[0], dy = y - d.start[1];
          // Snapping: the top left corner of the dragged node lands on the grid, its subclasses move by
          // the same offset (the tree keeps its shape). Shift inverts the setting.
          if (this.snap !== ev.shiftKey) {
            const g = this.S.grid.size, from = d.from[d.ids[0]];
            dx = Math.round((from.x + dx) / g) * g - from.x;
            dy = Math.round((from.y + dy) / g) * g - from.y;
          }
          for (const i of d.ids) this.pos[i] = { x: d.from[i].x + dx, y: d.from[i].y + dy };
          this._requestRedraw();
        } else if (this.pan) {
          this.view.tx = this.pan.tx + ev.clientX - this.pan.x;
          this.view.ty = this.pan.ty + ev.clientY - this.pan.y;
          this._applyView();
        }
      });
      const endPointer = () => { this.drag = null; this.pan = null; svg.classList.remove("panning"); };
      svg.addEventListener("pointerup", endPointer);
      svg.addEventListener("pointercancel", endPointer);

      const btn = cls => root.querySelector(cls);
      btn(".btn-zoom-in").onclick = () => this.zoomBy(1.2);
      btn(".btn-zoom-out").onclick = () => this.zoomBy(1 / 1.2);
      btn(".btn-fit").onclick = () => this.fit();
      btn(".btn-reset").onclick = () => this.reset();
      btn(".btn-snap").onclick = ev => {
        this.snap = !this.snap;
        ev.currentTarget.setAttribute("aria-pressed", String(this.snap));
      };
      btn(".btn-export").onclick = () => this.exportSvg();
      btn(".btn-help").onclick = ev => {
        const open = root.querySelector(".help-panel").classList.toggle("open");
        ev.currentTarget.setAttribute("aria-pressed", String(open));
      };
      btn(".legend-toggle").onclick = () => root.querySelector(".legend-panel").classList.toggle("collapsed");
      // container resized (window, notebook cell, iframe) -> fit again
      new ResizeObserver(() => { if (this.pos) this.fit(); }).observe(this);
    }
  }

  customElements.define("pipeline-view", PipelineView);
})();
