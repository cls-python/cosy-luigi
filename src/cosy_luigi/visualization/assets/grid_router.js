// Orthogonal edge router: a pure function without DOM and without ELK.
//
//   GridRouter.route(boxes, edges, options) -> { [edgeKey]: [[x, y], ...] }
//     boxes: [{id, x, y, w, h}]   (x, y = top left corner)
//     edges: [{key, src, dst, srcSides: ["E", ...], dstSides: ["W", ...]}]
//            sides in order of preference (E, W, N, S); every further side costs sidePenalty
//
// Approach:
//   1. Every box is enlarged by `margin` (obstacle). The obstacle borders, the ports and the center
//      lines between them form an irregular grid ("corridors").
//   2. For every edge, A* searches the cheapest path on the grid: length + cost per bend + extra cost
//      for corridors already used by other edges. Start and goal are short stubs in front of the ports
//      on the allowed sides.
//   3. Nudging: segments of different edges lying on top of each other on the same line are moved
//      apart by `laneSpacing`.
const GridRouter = (() => {
  const DEFAULTS = {
    margin: 14,          // minimum distance between edges and boxes
    bendCost: 40,        // cost per bend (in pixels of path length)
    sidePenalty: 120,    // extra cost per position in srcSides/dstSides (preferred side = 0)
    borderFactor: 1.15,  // running along an obstacle border costs slightly more than the corridor center
    crowdCost: 0.6,      // extra cost per pixel and edge already using it
    laneSpacing: 6,      // distance between parallel edges after nudging
  };
  const DIRS = [[1, 0], [-1, 0], [0, 1], [0, -1]];          // 0=E 1=W 2=S 3=N
  const SIDE_DIR = { E: 0, W: 1, S: 2, N: 3 };
  const OPPOSITE = [1, 0, 3, 2];

  // Port on a side. On top/bottom the center stays free for inheritance edges: the port lies on the
  // quarter facing the other box.
  function portPoint(b, side, towards) {
    const cx = b.x + b.w / 2, cy = b.y + b.h / 2;
    const qx = towards[0] >= cx ? b.x + b.w * 0.75 : b.x + b.w * 0.25;
    switch (side) {
      case "E": return [b.x + b.w, cy];
      case "W": return [b.x, cy];
      case "S": return [qx, b.y + b.h];
      case "N": return [qx, b.y];
    }
  }

  // ------------------------------------------------------------------ binary min-heap
  class Heap {
    constructor() { this.a = []; }
    get size() { return this.a.length; }
    push(f, v) {
      const a = this.a; a.push([f, v]);
      let i = a.length - 1;
      while (i > 0) { const p = (i - 1) >> 1; if (a[p][0] <= a[i][0]) break; [a[p], a[i]] = [a[i], a[p]]; i = p; }
    }
    pop() {
      const a = this.a, top = a[0], last = a.pop();
      if (a.length) {
        a[0] = last;
        let i = 0;
        for (;;) {
          const l = 2 * i + 1, r = l + 1;
          let m = i;
          if (l < a.length && a[l][0] < a[m][0]) m = l;
          if (r < a.length && a[r][0] < a[m][0]) m = r;
          if (m === i) break;
          [a[m], a[i]] = [a[i], a[m]]; i = m;
        }
      }
      return top;
    }
  }

  // ------------------------------------------------------------------ grid
  function buildGrid(boxes, ports, o) {
    const m = o.margin;
    const obstacles = boxes.map(b => ({ id: b.id, x1: b.x - m, y1: b.y - m, x2: b.x + b.w + m, y2: b.y + b.h + m }));
    // Tolerance, so that points on a border (e.g. the stubs in front of the ports) do not count as
    // "inside" because of rounding errors with fractional coordinates after dragging
    const EPS = 0.05;
    const inside = (x, y) => obstacles.some(r => x > r.x1 + EPS && x < r.x2 - EPS && y > r.y1 + EPS && y < r.y2 - EPS);

    const borderX = new Set(), borderY = new Set();
    for (const r of obstacles) { borderX.add(r.x1); borderX.add(r.x2); borderY.add(r.y1); borderY.add(r.y2); }
    let xs = [...borderX], ys = [...borderY];
    for (const p of ports) { xs.push(p.stub[0]); ys.push(p.stub[1]); }
    xs.push(Math.min(...xs) - 2 * m, Math.max(...xs) + 2 * m);
    ys.push(Math.min(...ys) - 2 * m, Math.max(...ys) + 2 * m);
    // Do not round coordinates (border points would end up slightly off the obstacle borders),
    // only merge nearly equal values
    const withMidlines = vals => {
      const s = [];
      for (const v of [...vals].sort((a, b) => a - b)) if (!s.length || v - s[s.length - 1] > 0.01) s.push(v);
      const out = [s[0]];
      for (let i = 1; i < s.length; i++) { if (s[i] - s[i - 1] > 4) out.push((s[i] + s[i - 1]) / 2); out.push(s[i]); }
      return out;
    };
    xs = withMidlines(xs); ys = withMidlines(ys);
    const isBorderX = xs.map(x => [...borderX].some(b => Math.abs(b - x) < 0.01));
    const isBorderY = ys.map(y => [...borderY].some(b => Math.abs(b - y) < 0.01));

    const nx = xs.length, ny = ys.length;
    const blocked = new Uint8Array(nx * ny);
    for (let j = 0; j < ny; j++) for (let i = 0; i < nx; i++) blocked[j * nx + i] = inside(xs[i], ys[j]) ? 1 : 0;
    // A segment between two neighboring points is free if both ends and its center are free
    // (obstacle borders are grid lines, so no obstacle lies "between" two neighbors)
    const segFree = new Int8Array(nx * ny * 2).fill(-1);
    function free(i, j, d) {      // d: 0 = to the right, 1 = downwards
      const k = (j * nx + i) * 2 + d;
      if (segFree[k] < 0) {
        const [i2, j2] = d === 0 ? [i + 1, j] : [i, j + 1];
        segFree[k] = !blocked[j * nx + i] && !blocked[j2 * nx + i2] &&
                     !inside((xs[i] + xs[i2]) / 2, (ys[j] + ys[j2]) / 2) ? 1 : 0;
      }
      return segFree[k] === 1;
    }
    const ix = x => xs.findIndex(v => Math.abs(v - x) < 0.01);
    const iy = y => ys.findIndex(v => Math.abs(v - y) < 0.01);
    return { xs, ys, nx, ny, blocked, free, ix, iy, isBorderX, isBorderY };
  }

  // ------------------------------------------------------------------ A*
  function astar(grid, starts, goals, usage, o) {
    const { xs, ys, nx, ny, free, isBorderX, isBorderY } = grid;
    const n = nx * ny * 4;
    const g = new Float64Array(n).fill(Infinity), parent = new Int32Array(n).fill(-1);
    const heap = new Heap();
    const h = (i, j) => Math.min(...goals.map(t => Math.abs(xs[i] - xs[t.i]) + Math.abs(ys[j] - ys[t.j])));
    const goalAt = new Map(goals.map(t => [t.j * nx + t.i, t]));

    for (const s of starts) {
      const st = (s.j * nx + s.i) * 4 + s.dir;
      if (s.cost < g[st]) { g[st] = s.cost; parent[st] = -1; heap.push(s.cost + h(s.i, s.j), st); }
    }
    let best = null;
    while (heap.size) {
      const [f, st] = heap.pop();
      if (best && f >= best.cost) break;
      const dir = st & 3, p = st >> 2, i = p % nx, j = (p / nx) | 0;
      if (f - h(i, j) > g[st] + 1e-9) continue;          // outdated heap entry
      const goal = goalAt.get(p);
      if (goal && dir !== goal.awayDir) {
        // at the goal stub the last segment enters the box; arriving differently costs a bend
        const total = g[st] + goal.cost + (dir === goal.inDir ? 0 : o.bendCost);
        if (!best || total < best.cost) best = { cost: total, state: st, goal };
      }
      for (let d = 0; d < 4; d++) {
        if (d === OPPOSITE[dir]) continue;
        const i2 = i + DIRS[d][0], j2 = j + DIRS[d][1];
        if (i2 < 0 || j2 < 0 || i2 >= nx || j2 >= ny) continue;
        const ok = d === 0 ? free(i, j, 0) : d === 1 ? free(i2, j2, 0) : d === 2 ? free(i, j, 1) : free(i2, j2, 1);
        if (!ok) continue;
        const horizontal = d < 2;
        const len = horizontal ? Math.abs(xs[i2] - xs[i]) : Math.abs(ys[j2] - ys[j]);
        const onBorder = horizontal ? isBorderY[j] : isBorderX[i];
        const segKey = horizontal ? `h${j}:${Math.min(i, i2)}` : `v${i}:${Math.min(j, j2)}`;
        const cost = len * (onBorder ? o.borderFactor : 1) + len * o.crowdCost * (usage.get(segKey) || 0) +
                     (d !== dir ? o.bendCost : 0);
        const st2 = ((j2 * nx + i2) << 2) + d;
        if (g[st] + cost < g[st2]) {
          g[st2] = g[st] + cost; parent[st2] = st;
          heap.push(g[st2] + h(i2, j2), st2);
        }
      }
    }
    if (!best) return null;
    const cells = [];
    for (let st = best.state; st >= 0; st = parent[st]) cells.push(st >> 2);
    cells.reverse();
    return { cells, goal: best.goal, start: starts.find(s => s.j * nx + s.i === cells[0]) };
  }

  // ------------------------------------------------------------------ helpers
  function simplify(pts) {
    const out = [pts[0]];
    for (let k = 1; k < pts.length; k++) {
      const p = pts[k], q = out[out.length - 1];
      if (Math.abs(p[0] - q[0]) < 0.01 && Math.abs(p[1] - q[1]) < 0.01) continue;
      if (out.length >= 2) {
        const r = out[out.length - 2];
        const colinear = (Math.abs(r[0] - q[0]) < 0.01 && Math.abs(q[0] - p[0]) < 0.01) ||
                         (Math.abs(r[1] - q[1]) < 0.01 && Math.abs(q[1] - p[1]) < 0.01);
        if (colinear) { out[out.length - 1] = p; continue; }
      }
      out.push(p);
    }
    return out;
  }

  // Move apart segments of different edges that overlap on the same line. A horizontal segment is only
  // moved in y, a vertical one only in x; so all segments stay orthogonal, even if both neighbors of a
  // point are moved.
  function nudge(routes, boxesById, o) {
    const segs = [];
    for (const [key, r] of Object.entries(routes)) {
      for (let k = 0; k + 1 < r.pts.length; k++) {
        const [a, b] = [r.pts[k], r.pts[k + 1]];
        const horizontal = Math.abs(a[1] - b[1]) < 0.01;
        const c = horizontal ? a[1] : a[0];
        const lo = horizontal ? Math.min(a[0], b[0]) : Math.min(a[1], b[1]);
        const hi = horizontal ? Math.max(a[0], b[0]) : Math.max(a[1], b[1]);
        // sort key: where the edge comes from (reduces crossings between lanes)
        const prev = r.pts[Math.max(0, k - 1)];
        segs.push({ key, k, horizontal, c: Math.round(c * 10) / 10, lo, hi, order: horizontal ? prev[1] : prev[0],
                    last: k + 1 === r.pts.length - 1, first: k === 0 });
      }
    }
    const groups = new Map();
    for (const s of segs) {
      const g = `${s.horizontal ? "h" : "v"}${s.c}`;
      if (!groups.has(g)) groups.set(g, []);
      groups.get(g).push(s);
    }
    const shift = new Map();       // `${key}|${k}` -> offset
    const maxOff = o.margin - 4;
    for (const list of groups.values()) {
      list.sort((a, b) => a.lo - b.lo);
      // build clusters of overlapping segments
      let cluster = [], end = -Infinity;
      const flush = () => {
        const keys = [...new Set(cluster.map(s => s.key))];
        if (keys.length > 1) {
          const order = Object.fromEntries(keys.map(kk => [kk, Math.min(...cluster.filter(s => s.key === kk).map(s => s.order))]));
          keys.sort((a, b) => order[a] - order[b] || (a < b ? -1 : 1));
          keys.forEach((kk, idx) => {
            const off = Math.max(-maxOff, Math.min(maxOff, (idx - (keys.length - 1) / 2) * o.laneSpacing));
            for (const s of cluster) if (s.key === kk) shift.set(`${s.key}|${s.k}`, off);
          });
        }
        cluster = []; end = -Infinity;
      };
      for (const s of list) {
        if (s.lo > end - 0.01 && cluster.length) flush();
        cluster.push(s); end = Math.max(end, s.hi);
      }
      flush();
    }
    for (const [key, r] of Object.entries(routes)) {
      const pts = r.pts.map(p => [...p]);
      for (let k = 0; k + 1 < r.pts.length; k++) {
        const off = shift.get(`${key}|${k}`);
        if (!off) continue;
        const horizontal = Math.abs(r.pts[k][1] - r.pts[k + 1][1]) < 0.01;
        const axis = horizontal ? 1 : 0;
        pts[k][axis] = r.pts[k][axis] + off;
        pts[k + 1][axis] = r.pts[k + 1][axis] + off;
      }
      // ports may only move along their side and not beyond the box
      for (const [idx, id] of [[0, r.src], [pts.length - 1, r.dst]]) {
        const b = boxesById[id], p = pts[idx];
        p[0] = Math.max(b.x + 4, Math.min(b.x + b.w - 4, p[0])) ;
        p[1] = Math.max(b.y + 4, Math.min(b.y + b.h - 4, p[1]));
        if (r.sides[idx === 0 ? 0 : 1] === "E") p[0] = b.x + b.w;
        if (r.sides[idx === 0 ? 0 : 1] === "W") p[0] = b.x;
        if (r.sides[idx === 0 ? 0 : 1] === "S") p[1] = b.y + b.h;
        if (r.sides[idx === 0 ? 0 : 1] === "N") p[1] = b.y;
      }
      r.pts = pts;
    }
  }

  // ------------------------------------------------------------------ main function
  function route(boxes, edges, options = {}) {
    const o = { ...DEFAULTS, ...options };
    const byId = Object.fromEntries(boxes.map(b => [b.id, b]));
    const center = b => [b.x + b.w / 2, b.y + b.h / 2];

    // port candidates per edge (point on the side + stub in front of it)
    const cands = edges.map(e => {
      const a = byId[e.src], b = byId[e.dst];
      const mk = (box, side, other, k) => {
        const p = portPoint(box, side, center(other)), d = DIRS[SIDE_DIR[side]];
        return { side, p, stub: [p[0] + d[0] * o.margin, p[1] + d[1] * o.margin], cost: k * o.sidePenalty };
      };
      return { e, src: e.srcSides.map((s, k) => mk(a, s, b, k)), dst: e.dstSides.map((s, k) => mk(b, s, a, k)) };
    });
    const grid = buildGrid(boxes, cands.flatMap(c => [...c.src, ...c.dst]), o);

    // short edges first: they have the fewest alternatives
    const dist = c => { const [a, b] = [center(byId[c.e.src]), center(byId[c.e.dst])]; return Math.abs(a[0] - b[0]) + Math.abs(a[1] - b[1]); };
    const order = [...cands].sort((p, q) => dist(p) - dist(q));

    const usage = new Map(), routes = {};
    for (const c of order) {
      const at = q => ({ i: grid.ix(q.stub[0]), j: grid.iy(q.stub[1]) });
      const starts = c.src.map(q => ({ ...at(q), dir: SIDE_DIR[q.side], cost: q.cost, q }))
                          .filter(s => s.i >= 0 && s.j >= 0 && !grid.blocked[s.j * grid.nx + s.i]);
      const goals = c.dst.map(q => ({ ...at(q), inDir: OPPOSITE[SIDE_DIR[q.side]], awayDir: SIDE_DIR[q.side], cost: q.cost, q }))
                         .filter(t => t.i >= 0 && t.j >= 0 && !grid.blocked[t.j * grid.nx + t.i]);
      const res = starts.length && goals.length ? astar(grid, starts, goals, usage, o) : null;
      if (!res) {
        // no free path (e.g. overlapping boxes): straight line between the preferred sides
        routes[c.e.key] = { pts: [c.src[0].p, c.dst[0].p], src: c.e.src, dst: c.e.dst, sides: [c.src[0].side, c.dst[0].side] };
        continue;
      }
      const pts = res.cells.map(cell => [grid.xs[cell % grid.nx], grid.ys[(cell / grid.nx) | 0]]);
      for (let k = 0; k + 1 < res.cells.length; k++) {
        const [p, q] = [res.cells[k], res.cells[k + 1]];
        const [i1, j1, i2, j2] = [p % grid.nx, (p / grid.nx) | 0, q % grid.nx, (q / grid.nx) | 0];
        const key = j1 === j2 ? `h${j1}:${Math.min(i1, i2)}` : `v${i1}:${Math.min(j1, j2)}`;
        usage.set(key, (usage.get(key) || 0) + 1);
      }
      routes[c.e.key] = { pts: simplify([res.start.q.p, ...pts, res.goal.q.p]), src: c.e.src, dst: c.e.dst,
                          sides: [res.start.q.side, res.goal.q.side] };
    }

    nudge(routes, byId, o);
    return Object.fromEntries(Object.entries(routes).map(([k, r]) => [k, simplify(r.pts)]));
  }

  return { route, DEFAULTS };
})();
