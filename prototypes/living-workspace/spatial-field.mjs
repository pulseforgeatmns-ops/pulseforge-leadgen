// Prototype-only Living Business Field. The tenant is the conceptual centre (a
// quiet context core, not a light source); domains are glass nodes connected by
// the relationships the fixtures declare; a focused domain draws its business
// objects forward and lets the rest recede. Prominence comes from priority,
// status, relevance and selection. Nothing moves while idle: transitions run only
// when the business context changes. Every interaction is a dispatcher command.
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
const svgNS = 'http://www.w3.org/2000/svg';
// Overview anchors (fractions of the field), asymmetric so the composition reads as designed.
const ANCHORS = { acquisition: [0.25, 0.68], content: [0.34, 0.25], campaigns: [0.68, 0.26], clients: [0.76, 0.7] };
const COMPACT = { acquisition: [0.27, 0.74], content: [0.27, 0.26], campaigns: [0.73, 0.26], clients: [0.73, 0.74] };
const GLYPHS = { content: '¶', acquisition: '◎', clients: '◇', campaigns: '↗' };
export function createSpatialField({ host, orbit, connections, list, dispatch, scope, onFocusDomain, onSelectObject, onOverview }) {
  let current = null, tenant = '', view = { x: 0, y: 0, zoom: 1 }, start = null, wheelTimer = null, listSignature = '';
  host.tabIndex = 0;
  host.setAttribute('aria-label', 'Business field. Drag the background or use arrow keys to pan; use the mouse wheel or the zoom buttons to zoom; Escape returns to the overview.');
  const change = (next, token = scope()) => dispatch({ type: 'set_field_view', source: 'human', ...next, ...token });
  const applyView = v => { orbit.style.transform = `translate(${v.x}px, ${v.y}px) scale(${v.zoom})`; };

  host.addEventListener('pointerdown', e => {
    if (e.target.closest('button') || e.button !== 0) return;
    start = { id: e.pointerId, x: e.clientX, y: e.clientY, view: { ...view }, scope: scope() };
    host.setPointerCapture(e.pointerId);
  });
  host.addEventListener('pointermove', e => {
    if (!start || start.id !== e.pointerId) return;
    applyView({ ...view, x: start.view.x + e.clientX - start.x, y: start.view.y + e.clientY - start.y });
  });
  function finish(e) {
    if (!start || start.id !== e.pointerId) return;
    const prior = start; start = null;
    if (host.hasPointerCapture(e.pointerId)) host.releasePointerCapture(e.pointerId);
    if (e.type === 'pointercancel') applyView(view);
    else if (e.clientX !== prior.x || e.clientY !== prior.y) change({ ...view, x: prior.view.x + e.clientX - prior.x, y: prior.view.y + e.clientY - prior.y }, prior.scope);
  }
  host.addEventListener('pointerup', finish); host.addEventListener('pointercancel', finish);
  // Wheel zoom previews immediately and commits one undoable view change once the wheel settles.
  let pending = null;
  host.addEventListener('wheel', e => {
    e.preventDefault();
    pending = { ...(pending || view), zoom: clamp((pending || view).zoom * (e.deltaY < 0 ? 1.08 : 1 / 1.08), 0.7, 1.6) };
    applyView(pending);
    const token = scope();
    clearTimeout(wheelTimer);
    wheelTimer = setTimeout(() => { const v = pending; pending = null; change(v, token); }, 220);
  }, { passive: false });
  host.addEventListener('keydown', e => {
    if (e.key === 'Escape' && current?.state.fieldFocus && !current.state.maxOpen) {
      e.preventDefault(); const domainId = current.state.fieldFocus; onOverview();
      orbit.querySelector(`.domain-node[data-domain-id="${domainId}"]`)?.focus({ preventScroll: true });
      return;
    }
    if (e.target !== host) return;
    const delta = { ArrowLeft: [-25, 0], ArrowRight: [25, 0], ArrowUp: [0, -25], ArrowDown: [0, 25] }[e.key];
    if (delta) { e.preventDefault(); change({ ...view, x: view.x + delta[0], y: view.y + delta[1] }); }
  });

  function make(tag, className, text) {
    const el = document.createElement(tag);
    if (className) el.className = className;
    if (text) el.textContent = text;
    return el;
  }
  function link(cls, data) {
    const path = document.createElementNS(svgNS, 'path');
    path.setAttribute('class', `link ${cls}`);
    for (const [k, v] of Object.entries(data)) path.dataset[k] = v;
    connections.append(path); return path;
  }
  function workingObjects(fixture, state) {
    const objects = fixture.records.filter(r => r.kind === 'object');
    if (state.lens !== 'entities') return objects.filter(r => r.domain === state.fieldFocus);
    const q = state.entityQuery.trim().toLowerCase();
    return objects.filter(r => (state.entityDomain === 'all' || r.domain === state.entityDomain)
      && (!q || `${r.name} ${r.type} ${r.stage} ${r.summary}`.toLowerCase().includes(q)));
  }
  function build(fixture, state) {
    tenant = fixture.id; start = null;
    orbit.querySelectorAll('.context-core, .domain-node, .object-node').forEach(n => n.remove());
    connections.replaceChildren(); list.replaceChildren(); listSignature = '';
    const objects = fixture.records.filter(r => r.kind === 'object');
    // A field is a working set, never a portfolio dump. Rendering is bounded even
    // when a fixture is scaled to thousands of companies/contacts.
    const visibleObjects = workingObjects(fixture, state).slice(0, state.nodeBudget);
    const core = make('button', 'context-core'); core.type = 'button'; core.dataset.core = fixture.id;
    core.setAttribute('aria-label', `${fixture.name}: workspace context. Return to overview`);
    core.append(make('small', '', 'WORKSPACE'), make('strong', '', fixture.name), make('span', 'core-meta', `${fixture.overview.domains.length} domains · ${objects.length} records`));
    core.addEventListener('click', () => onOverview());
    orbit.append(core);
    for (const domain of fixture.overview.domains) link('core-link', { domain: domain.id });
    for (const l of fixture.overview.links) link('domain-link', { from: l.from, to: l.to });
    for (const domain of fixture.overview.domains) {
      const own = objects.filter(o => o.domain === domain.id);
      const record = fixture.records.find(r => r.kind === 'domain' && r.domain === domain.id);
      const node = make('button', `domain-node priority-${domain.priority} status-${domain.status}`); node.type = 'button';
      node.dataset.domainId = domain.id;
      node.setAttribute('aria-label', `${domain.label}, ${domain.priority} priority, ${domain.note}. ${own.length} ${domain.objectPlural}. ${record.summary}`);
      const top = make('span', 'node-top');
      top.append(make('span', 'node-glyph', GLYPHS[domain.id]), make('strong', '', domain.label), make('i', 'status-dot'));
      const meta = make('span', 'node-meta', `${own.length} ${domain.objectPlural}${domain.priority === 'elevated' || domain.priority === 'urgent' ? ` · ${domain.priority}` : ''}`);
      node.append(top, meta, make('span', 'node-note', domain.note));
      node.addEventListener('click', () => onFocusDomain(domain.id));
      orbit.append(node);
      for (const obj of own.filter(obj => visibleObjects.some(v => v.id === obj.id))) {
        link('object-link', { object: obj.id, domain: domain.id });
        const o = make('button', `object-node status-${obj.status}`); o.type = 'button';
        o.dataset.objectId = obj.id; o.dataset.parentDomain = domain.id;
        o.setAttribute('aria-label', `${obj.type}: ${obj.name}. ${obj.stage}. ${obj.summary}`);
        const row = make('span', 'node-top'); row.append(make('i', 'status-dot'), make('strong', '', obj.name));
        o.append(row, make('span', 'node-meta', `${obj.type} · ${obj.stage}`));
        o.addEventListener('click', () => onSelectObject(obj));
        orbit.append(o);
      }
    }
  }
  const curve = (a, b, bend = 0) => {
    const mx = (a[0] + b[0]) / 2, my = (a[1] + b[1]) / 2;
    const dx = b[0] - a[0], dy = b[1] - a[1], len = Math.hypot(dx, dy) || 1;
    const cx = mx - dy / len * bend, cy = my + dx / len * bend;
    return `M${a[0].toFixed(1)} ${a[1].toFixed(1)} Q${cx.toFixed(1)} ${cy.toFixed(1)} ${b[0].toFixed(1)} ${b[1].toFixed(1)}`;
  };
  function setPath(path, d, visible) {
    path.setAttribute('d', d); path.style.d = `path("${d}")`;
    path.dataset.visible = String(visible);
  }
  function place(node, x, y, width, extra = {}) {
    node.style.left = `${x}px`; node.style.top = `${y}px`; node.style.width = `${width}px`;
    for (const [k, v] of Object.entries(extra)) node.dataset[k] = v;
  }
  function position() {
    if (!current) return;
    const { fixture, state } = current;
    const w = host.clientWidth, h = host.clientHeight;
    if (!w || !h) return;
    connections.setAttribute('viewBox', `0 0 ${w} ${h}`);
    const focus = state.fieldFocus, compact = w < 560;
    host.dataset.level = focus ? 'domain' : 'overview';
    host.classList.toggle('compact-field', compact);
    const selected = fixture.records.find(r => r.id === state.selected);
    const pos = {};
    // Context core
    const core = orbit.querySelector('.context-core');
    const coreW = clamp(w * 0.14, 140, 196);
    pos.core = focus ? [Math.min(coreW * 0.42, 86) + 12, 24] : [w / 2, h / 2];
    place(core, ...pos.core, focus ? Math.min(coreW * 0.84, 172) : coreW, { state: focus ? 'distant' : 'center' });
    // Domains
    const cardW = compact ? clamp(w * 0.38, 118, 168) : clamp(w * 0.15, 150, 214);
    const others = fixture.overview.domains.filter(d => d.id !== focus);
    for (const domain of fixture.overview.domains) {
      const node = orbit.querySelector(`.domain-node[data-domain-id="${domain.id}"]`);
      let x, y, width = cardW, stateName;
      if (!focus) {
        const [fx, fy] = (compact ? COMPACT : ANCHORS)[domain.id];
        x = w * fx; y = h * fy; stateName = 'overview';
        if (domain.priority === 'elevated' || domain.priority === 'urgent') width = cardW * 1.08;
      } else if (domain.id === focus) {
        x = compact ? w * 0.27 : clamp(w * 0.2, cardW * 0.6 + 14, w * 0.3); y = h * 0.5; width = cardW * 1.12; stateName = 'focus';
      } else {
        const i = others.indexOf(domain), rw = clamp(w * 0.11, 96, 140);
        x = w - rw / 2 - 12; y = h * (0.22 + i * 0.28); width = rw; stateName = 'recede';
      }
      pos[domain.id] = [x, y, width];
      place(node, x, y, width, { state: stateName });
      node.dataset.related = String(!!(selected?.related?.some(r => fixture.records.find(x2 => x2.id === r.recordId)?.domain === domain.id) && domain.id !== focus));
    }
    // Objects of the focused domain gather to its right on a gentle arc.
    const chipW = compact ? clamp(w * 0.36, 118, 170) : clamp(w * 0.19, 150, 232);
    for (const domain of fixture.overview.domains) {
      const nodes = [...orbit.querySelectorAll(`.object-node[data-parent-domain="${domain.id}"]`)];
      const n = nodes.length, visible = focus === domain.id;
      const spread = Math.min(h * 0.36, (n - 1) * 34);
      nodes.forEach((node, i) => {
        const t = n === 1 ? 0 : (i / (n - 1)) * 2 - 1;
        const [dx, dy] = pos[domain.id];
        const x = visible ? (compact ? w * 0.68 : Math.min(w * 0.52, w - 170 - chipW / 2)) + (1 - Math.abs(t)) * (compact ? 8 : w * 0.035) : dx;
        const y = visible ? h * 0.5 + t * spread : dy;
        pos[node.dataset.objectId] = [x, y, chipW];
        node.inert = !visible;
        place(node, x, y, visible ? chipW : chipW * 0.6, { visible: String(visible) });
        node.classList.toggle('selected', node.dataset.objectId === state.selected);
        node.setAttribute('aria-pressed', String(node.dataset.objectId === state.selected));
      });
    }
    // Links: core ↔ domains, declared domain relationships, domain ↔ objects, selection relationships.
    const edge = (id, toward) => { const [x, y, wd] = pos[id]; return [x + Math.sign(toward[0] - x) * Math.min(wd / 2, Math.abs(toward[0] - x) / 2), y]; };
    connections.querySelectorAll('.core-link').forEach(p => {
      const d = p.dataset.domain;
      setPath(p, curve(pos.core, pos[d], focus ? 0 : (pos[d][1] < h / 2 ? -18 : 18)), !focus || d === focus);
      p.dataset.strength = fixture.overview.domains.find(x => x.id === d).priority;
    });
    connections.querySelectorAll('.domain-link').forEach(p => {
      const a = pos[p.dataset.from], b = pos[p.dataset.to];
      setPath(p, curve(a, b, focus ? 0 : 36), !focus);
    });
    connections.querySelectorAll('.relation-link, .relation-label').forEach(n => n.remove());
    connections.querySelectorAll('.object-link').forEach(p => {
      const visible = focus === p.dataset.domain;
      const a = edge(p.dataset.domain, pos[p.dataset.object]), b = pos[p.dataset.object];
      setPath(p, curve(a, [b[0] - (visible ? b[2] / 2 : 0), b[1]], 0), visible);
      p.dataset.selected = String(p.dataset.object === state.selected);
    });
    if (focus && selected?.kind === 'object' && selected.domain === focus) {
      for (const rel of selected.related || []) {
        const target = fixture.records.find(r => r.id === rel.recordId); if (!target) continue;
        const toId = target.domain === focus ? target.id : target.domain;
        if (!pos[toId] || toId === selected.id) continue;
        const a = pos[selected.id], b = pos[toId], p = link('relation-link', { to: toId });
        const label = document.createElementNS(svgNS, 'text');
        label.setAttribute('class', 'relation-label'); label.textContent = rel.relation.toLowerCase();
        if (target.domain === focus) {
          // Sibling relation: arc out to the right of the cluster, label at the apex.
          const from = [a[0] + a[2] / 2, a[1]], to = [b[0] + b[2] / 2, b[1]], apex = Math.max(from[0], to[0]) + 54;
          const d = `M${from[0].toFixed(1)} ${from[1].toFixed(1)} C${apex.toFixed(1)} ${from[1].toFixed(1)} ${apex.toFixed(1)} ${to[1].toFixed(1)} ${to[0].toFixed(1)} ${to[1].toFixed(1)}`;
          setPath(p, d, true);
          label.setAttribute('x', (apex - 8).toFixed(1)); label.setAttribute('y', ((from[1] + to[1]) / 2 + 3).toFixed(1)); label.setAttribute('text-anchor', 'start');
        } else {
          const from = [a[0] + (b[0] > a[0] ? a[2] / 2 : -a[2] / 2), a[1]], to = [b[0] - (b[0] > a[0] ? b[2] / 2 : -b[2] / 2), b[1]];
          setPath(p, curve(from, to, -24), true);
          label.setAttribute('x', ((from[0] + to[0]) / 2).toFixed(1)); label.setAttribute('y', ((from[1] + to[1]) / 2 - 8).toFixed(1)); label.setAttribute('text-anchor', 'middle');
        }
        connections.append(label);
      }
    }
  }
  new ResizeObserver(position).observe(host);

  function renderList(fixture, state) {
    const signature = `${fixture.id}|${state.lens}|${state.entityQuery}|${state.entityDomain}|${state.fieldFocus}|${state.selected}`;
    if (signature === listSignature) return;
    listSignature = signature;
    const rows = [];
    const row = (text, onClick, data = {}) => {
      const b = make('button', '', text); b.type = 'button';
      Object.assign(b.dataset, data); b.addEventListener('click', onClick); rows.push(b); return b;
    };
    if (state.lens === 'objectives') {
      for (const domain of fixture.overview.domains) row(`${domain.label} objective · ${domain.note} · owner: Fixture operator`, () => onFocusDomain(domain.id), { domainId: domain.id });
    } else if (state.lens === 'entities') {
      const matches = workingObjects(fixture, state), visible = matches.slice(0, state.nodeBudget);
      row(`${matches.length} matching entities · showing ${visible.length} in the working set`, () => {}, { aggregate: '' });
      for (const obj of visible) row(`${obj.type} · ${obj.name} — ${obj.stage}. ${obj.summary}`, () => onSelectObject(obj), { objectId: obj.id });
      if (matches.length > visible.length) row(`+ ${matches.length - visible.length} more matches — refine search or filter`, () => {}, { overflow: '' });
    } else if (!state.fieldFocus) {
      for (const domain of fixture.overview.domains) {
        const record = fixture.records.find(r => r.kind === 'domain' && r.domain === domain.id);
        const count = fixture.records.filter(r => r.kind === 'object' && r.domain === domain.id).length;
        row(`${domain.label} · ${domain.priority} · ${count} ${domain.objectPlural} — ${record.summary}`, () => onFocusDomain(domain.id), { domainId: domain.id });
      }
    } else {
      const domain = fixture.overview.domains.find(d => d.id === state.fieldFocus);
      row(`← ${fixture.name} overview`, () => onOverview(), { listBack: '' });
      row(`${domain.label} · ${domain.summary.compressed}`, () => onFocusDomain(domain.id), { domainId: domain.id });
      for (const obj of fixture.records.filter(r => r.kind === 'object' && r.domain === domain.id)) {
        row(`${obj.type} · ${obj.name} — ${obj.stage}. ${obj.summary}`, () => onSelectObject(obj), { objectId: obj.id });
      }
    }
    list.replaceChildren(...rows);
  }

  function render(fixture, state) {
    const fresh = tenant !== fixture.id;
    if (fresh || current?.state.lens !== state.lens || current?.state.entityQuery !== state.entityQuery || current?.state.entityDomain !== state.entityDomain) { build(fixture, state); host.classList.add('settling'); }
    current = { fixture, state };
    view = state.fieldView;
    if (!start && !pending) applyView(view);
    position();
    renderList(fixture, state);
    // A newly built system appears in place; camera transitions apply to navigation only.
    if (fresh) requestAnimationFrame(() => requestAnimationFrame(() => host.classList.remove('settling')));
    const selected = fixture.records.find(r => r.id === state.selected);
    orbit.querySelectorAll('.domain-node').forEach(n => n.classList.toggle('selected', n.dataset.domainId === selected?.domain));
    list.querySelectorAll('[data-domain-id]').forEach(n => n.setAttribute('aria-pressed', String(!state.fieldFocus && n.dataset.domainId === selected?.domain)));
    list.querySelectorAll('[data-object-id]').forEach(n => n.setAttribute('aria-pressed', String(n.dataset.objectId === state.selected)));
  }
  return { render };
}
