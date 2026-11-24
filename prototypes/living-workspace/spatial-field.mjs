// Prototype-only orbital presentation. The sun is the active tenant context;
// domains are planets on orbital paths; business objects orbit a focused
// domain. Domain priorities are fixture inputs, never recomputed: they only
// choose the orbit (elevated/urgent sit closer to the sun). Every interaction
// is a presentation command through the shared dispatcher.
const svgNS = 'http://www.w3.org/2000/svg';
const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
const RINGS = [0.58, 0.8, 1];
const ringFor = priority => ({ urgent: 0, elevated: 0, normal: 1, monitored: 2 })[priority] ?? 1;
// Fixed, asymmetric angles (degrees, 0 = right, 90 = down) so the system reads as composed, not a grid.
const ANGLES = { acquisition: 165, content: 245, clients: 15, campaigns: 310 };
const GLYPHS = { content: '✧', acquisition: '⌖', clients: '◉', campaigns: '◇' };

export function createSpatialField({ host, orbit, connections, list, dispatch, scope, onFocusDomain, onSelectObject, onOverview }) {
  let current = null, tenant = '', view = { x: 0, y: 0, zoom: 1 }, start = null, wheelTimer = null, listSignature = '';
  host.tabIndex = 0;
  host.setAttribute('aria-label', 'Orbital exploration field. Drag the background or use arrow keys to pan; use the mouse wheel or the zoom buttons to zoom; Escape returns to the overview.');
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
      orbit.querySelector(`.planet[data-domain-id="${domainId}"]`)?.focus({ preventScroll: true });
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
  function svg(tag, attrs) {
    const el = document.createElementNS(svgNS, tag);
    for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
    return el;
  }
  function build(fixture) {
    tenant = fixture.id; start = null;
    orbit.querySelectorAll('.sun, .planet, .moon, .moon-hint').forEach(n => n.remove());
    connections.replaceChildren(); list.replaceChildren(); listSignature = '';
    RINGS.forEach((_, i) => connections.append(svg('ellipse', { class: 'orbit-path', 'data-ring': i })));
    connections.append(svg('path', { class: 'tether' }), svg('ellipse', { class: 'object-orbit' }));
    const sun = make('button', 'sun'); sun.type = 'button'; sun.dataset.sun = fixture.id;
    sun.setAttribute('aria-label', `${fixture.name}: workspace context. Return to overview`);
    const corona = make('span', 'sun-body'); corona.setAttribute('aria-hidden', 'true');
    const sunLabel = make('span', 'body-label');
    sunLabel.append(make('small', '', 'WORKSPACE CONTEXT'), make('strong', '', fixture.name));
    sun.append(corona, sunLabel); sun.addEventListener('click', () => onOverview());
    orbit.append(sun);
    for (const domain of fixture.overview.domains) {
      const objects = fixture.records.filter(r => r.kind === 'object' && r.domain === domain.id);
      const record = fixture.records.find(r => r.kind === 'domain' && r.domain === domain.id);
      const planet = make('button', `planet priority-${domain.priority}`); planet.type = 'button';
      planet.dataset.domainId = domain.id;
      planet.setAttribute('aria-label', `${domain.label}, ${domain.priority} priority, ${objects.length} ${domain.objectPlural} in orbit. ${record.summary}`);
      const body = make('span', 'planet-body'); body.setAttribute('aria-hidden', 'true');
      body.append(make('span', 'planet-glyph', GLYPHS[domain.id]));
      const label = make('span', 'body-label');
      label.append(make('strong', '', domain.label), make('small', '', domain.priority === 'elevated' || domain.priority === 'urgent'
        ? `↑ ${domain.priority.toUpperCase()} · ${objects.length} ${domain.objectPlural}` : `${objects.length} ${domain.objectPlural}`));
      planet.append(body, label); planet.addEventListener('click', () => onFocusDomain(domain.id));
      orbit.append(planet);
      objects.forEach((obj, i) => {
        const hint = make('span', 'moon-hint'); hint.dataset.parentDomain = domain.id; hint.dataset.index = i;
        hint.setAttribute('aria-hidden', 'true'); orbit.append(hint);
        const moon = make('button', 'moon'); moon.type = 'button';
        moon.dataset.objectId = obj.id; moon.dataset.parentDomain = domain.id;
        moon.setAttribute('aria-label', `${obj.type}: ${obj.name}. ${obj.stage}. ${obj.summary}`);
        const moonBody = make('span', 'moon-body'); moonBody.setAttribute('aria-hidden', 'true');
        const moonLabel = make('span', 'body-label');
        moonLabel.append(make('strong', '', obj.name), make('small', '', `${obj.type} · ${obj.stage}`));
        moon.append(moonBody, moonLabel); moon.addEventListener('click', () => onSelectObject(obj));
        orbit.append(moon);
      });
    }
  }
  function place(node, x, y, size, extra = {}) {
    node.style.left = `${x}px`; node.style.top = `${y}px`;
    node.style.setProperty('--size', `${size}px`);
    for (const [k, v] of Object.entries(extra)) node.style.setProperty(k, v);
  }
  // Highlight faces the light source, so every body reads as lit by the sun (or its planet).
  const light = (x, y, lx, ly) => {
    const d = Math.hypot(lx - x, ly - y) || 1;
    return { '--lx': `${50 + 26 * (lx - x) / d}%`, '--ly': `${50 + 26 * (ly - y) / d}%` };
  };
  function position() {
    if (!current) return;
    const { fixture, state } = current;
    const w = host.clientWidth, h = host.clientHeight;
    if (!w || !h) return;
    connections.setAttribute('viewBox', `0 0 ${w} ${h}`);
    const cx = w / 2, cy = h / 2, focus = state.fieldFocus;
    const compact = w < 560;
    host.dataset.level = focus ? 'domain' : 'overview';
    host.classList.toggle('compact-field', compact);
    const rx = Math.min(w * 0.42, h * 2.3), ry = Math.min(h * 0.36, rx * 0.72);
    connections.querySelectorAll('.orbit-path').forEach((ring, i) => {
      const scale = RINGS[i] * (focus ? 1.25 : 1);
      ring.setAttribute('cx', focus ? clamp(w * 0.07, 30, 90) : cx); ring.setAttribute('cy', cy);
      ring.setAttribute('rx', rx * scale); ring.setAttribute('ry', ry * scale);
    });
    const sun = orbit.querySelector('.sun');
    const sunPos = focus ? [clamp(w * 0.07, 30, 90), cy, 30] : [cx, cy, clamp(h * 0.26, 44, 96)];
    place(sun, ...sunPos);
    sun.dataset.state = focus ? 'distant' : 'center';
    const others = fixture.overview.domains.filter(d => d.id !== focus);
    const planetPos = {};
    fixture.overview.domains.forEach(domain => {
      const node = orbit.querySelector(`.planet[data-domain-id="${domain.id}"]`);
      let x, y, size, depth = 1;
      if (!focus) {
        const a = ANGLES[domain.id] * Math.PI / 180, r = RINGS[ringFor(domain.priority)];
        x = cx + rx * r * Math.cos(a); y = cy + ry * r * Math.sin(a);
        depth = 0.86 + 0.14 * (Math.sin(a) + 1) / 2; // nearer (lower) bodies read larger
        size = clamp(h * 0.2, 44, 76) * depth;
        node.dataset.state = 'orbit';
      } else if (domain.id === focus) {
        x = cx; y = cy; size = clamp(h * 0.34, 62, 124);
        node.dataset.state = 'focus';
      } else {
        const i = others.indexOf(domain);
        x = w - clamp(w * 0.05, 28, 58); y = h * (0.22 + i * 0.28); size = 26;
        node.dataset.state = 'recede';
      }
      planetPos[domain.id] = [x, y, size];
      node.style.zIndex = String(node.dataset.state === 'focus' ? 30 : Math.round(10 + y / 10));
      node.style.setProperty('--depth', depth.toFixed(3));
      node.dataset.labelSide = node.dataset.state === 'recede' ? 'left' : 'below';
      place(node, x, y, size, light(x, y, sunPos[0], sunPos[1]));
    });
    const tether = connections.querySelector('.tether'), objectOrbit = connections.querySelector('.object-orbit');
    const orx = clamp(Math.min(w * 0.3, h * 1.6), 90, 440), ory = Math.min(h * 0.37, orx * 0.6);
    if (focus) {
      const [px, py] = planetPos[focus];
      tether.setAttribute('d', `M${sunPos[0]} ${sunPos[1]} Q${(sunPos[0] + px) / 2} ${py - h * 0.18} ${px} ${py}`);
      Object.entries({ cx: px, cy: py, rx: orx, ry: ory }).forEach(([k, v]) => objectOrbit.setAttribute(k, v));
    }
    orbit.querySelectorAll('.moon').forEach(moon => {
      const domainId = moon.dataset.parentDomain;
      const siblings = [...orbit.querySelectorAll(`.moon[data-parent-domain="${domainId}"]`)];
      const i = siblings.indexOf(moon), n = siblings.length;
      const [px, py, psize] = planetPos[domainId];
      const visible = focus === domainId;
      moon.inert = !visible; moon.dataset.visible = String(visible);
      if (visible) {
        const a = (160 + i * 360 / n) * Math.PI / 180; // keeps the planet label (below) and tether (above-left) clear
        const x = px + orx * Math.cos(a), y = py + ory * Math.sin(a);
        moon.dataset.labelSide = compact ? 'below' : Math.cos(a) >= 0 ? 'right' : 'left';
        moon.style.zIndex = String(Math.sin(a) > 0 ? 40 : 20);
        place(moon, x, y, 30, light(x, y, px, py));
      } else place(moon, px, py, 8, light(px, py, sunPos[0], sunPos[1]));
      const hint = orbit.querySelector(`.moon-hint[data-parent-domain="${domainId}"][data-index="${i}"]`);
      const ha = (-60 + i * 50) * Math.PI / 180, hr = psize * 0.78;
      hint.dataset.visible = String(!focus);
      place(hint, px + hr * Math.cos(ha), py + hr * 0.55 * Math.sin(ha), 5);
    });
  }
  new ResizeObserver(position).observe(host);

  function renderList(fixture, state) {
    const signature = `${fixture.id}|${state.fieldFocus}|${state.selected}`;
    if (signature === listSignature) return;
    listSignature = signature;
    const rows = [];
    const row = (text, onClick, data = {}) => {
      const b = make('button', '', text); b.type = 'button';
      Object.assign(b.dataset, data); b.addEventListener('click', onClick); rows.push(b); return b;
    };
    if (!state.fieldFocus) {
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
    if (fresh) { build(fixture); host.classList.add('settling'); }
    current = { fixture, state };
    view = state.fieldView;
    if (!start && !pending) applyView(view);
    position();
    renderList(fixture, state);
    // A newly built system appears in place; camera transitions apply to navigation only.
    if (fresh) requestAnimationFrame(() => requestAnimationFrame(() => host.classList.remove('settling')));
    const selected = fixture.records.find(r => r.id === state.selected);
    orbit.querySelectorAll('.planet').forEach(n => n.classList.toggle('selected', n.dataset.domainId === selected?.domain));
    orbit.querySelectorAll('.moon').forEach(n => {
      const on = n.dataset.objectId === state.selected;
      n.classList.toggle('selected', on); n.setAttribute('aria-pressed', String(on));
    });
    list.querySelectorAll('[data-domain-id]').forEach(n => n.setAttribute('aria-pressed', String(!state.fieldFocus && n.dataset.domainId === selected?.domain)));
    list.querySelectorAll('[data-object-id]').forEach(n => n.setAttribute('aria-pressed', String(n.dataset.objectId === state.selected)));
  }
  return { render };
}
