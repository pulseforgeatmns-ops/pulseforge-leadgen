// Prototype-only spatial presentation. Domain priorities are fixture inputs, never recomputed.
export function createSpatialField({ host, orbit, connections, list, dispatch, scope, onOpen, onInvestigate }) {
  const positions = { content: [24, 19], acquisition: [80, 35], clients: [68, 78], campaigns: [20, 65] };
  let tenant = '', view = { x: 0, y: 0, zoom: 1 }, start;
  const svgNS = 'http://www.w3.org/2000/svg';
  host.tabIndex = 0; host.setAttribute('aria-label', 'Exploration field. Drag background to pan; use arrow keys to pan.');
  function change(next, token = scope()) { dispatch({ type: 'set_field_view', source: 'human', ...next, ...token }); }
  host.addEventListener('pointerdown', e => {
    if (e.target.closest('button') || e.button !== 0) return;
    start = { id: e.pointerId, x: e.clientX, y: e.clientY, view: { ...view }, scope: scope() };
    host.setPointerCapture(e.pointerId);
  });
  host.addEventListener('pointermove', e => {
    if (!start || start.id !== e.pointerId) return;
    orbit.style.transform = `translate(${start.view.x + e.clientX - start.x}px, ${start.view.y + e.clientY - start.y}px) scale(${view.zoom})`;
  });
  function finish(e) {
    if (!start || start.id !== e.pointerId) return;
    const prior = start; start = null;
    if (host.hasPointerCapture(e.pointerId)) host.releasePointerCapture(e.pointerId);
    if (e.type === 'pointercancel') orbit.style.transform = `translate(${view.x}px, ${view.y}px) scale(${view.zoom})`;
    else change({ ...view, x: prior.view.x + e.clientX - prior.x, y: prior.view.y + e.clientY - prior.y }, prior.scope);
  }
  host.addEventListener('pointerup', finish); host.addEventListener('pointercancel', finish);
  host.addEventListener('keydown', e => {
    if (e.target !== host) return;
    const delta = { ArrowLeft: [-25, 0], ArrowRight: [25, 0], ArrowUp: [0, -25], ArrowDown: [0, 25] }[e.key];
    if (delta) { e.preventDefault(); change({ ...view, x: view.x + delta[0], y: view.y + delta[1] }); }
  });
  function render(fixture, state) {
    view = state.fieldView;
    orbit.style.transform = `translate(${view.x}px, ${view.y}px) scale(${view.zoom})`;
    if (tenant !== fixture.id) {
      tenant = fixture.id; start = null;
      orbit.querySelectorAll('button').forEach(n => n.remove()); connections.replaceChildren(); list.replaceChildren();
      orbit.querySelector('.field-origin strong').textContent = fixture.name;
      for (const domain of fixture.overview.domains) {
        const record = fixture.records.find(r => r.domain === domain.id);
        const raw = positions[domain.id];
        const proximity = { urgent: .66, elevated: .82, normal: 1, monitored: 1.08 }[domain.priority];
        const x = 48 + (raw[0] - 48) * proximity, y = 51 + (raw[1] - 51) * proximity;
        const node = document.createElement('button'); node.className = `spatial-node priority-${domain.priority}`; node.dataset.domainId = domain.id;
        node.style.left = `${x}%`; node.style.top = `${y}%`;
        node.setAttribute('aria-label', `${domain.label}, ${domain.priority}. ${record.summary}`);
        const icon = document.createElement('span'); icon.className = 'node-symbol'; icon.textContent = { content: '✧', acquisition: '⌖', clients: '◉', campaigns: '◇' }[domain.id];
        const label = document.createElement('strong'); label.textContent = domain.label;
        const priority = document.createElement('small'); priority.textContent = domain.priority === 'elevated' ? '↑ ELEVATED ATTENTION' : domain.priority.toUpperCase();
        const summary = document.createElement('span'); summary.className = 'node-summary'; summary.textContent = domain.summary.compressed;
        node.append(icon, priority, label, summary); node.addEventListener('click', () => onOpen(record)); orbit.append(node);
        const item = document.createElement('button'); item.className = 'record-satellite'; item.dataset.recordId = record.id;
        item.style.left = `${x}%`; item.style.top = `${y + 15}%`;
        item.textContent = `${domain.id === 'acquisition' ? '3 businesses' : domain.id === 'content' ? '2 draft ideas' : 'Review notes'} +`;
        item.setAttribute('aria-label', `Investigate ${record.name}`); item.addEventListener('click', () => onInvestigate(record)); orbit.append(item);
        const path = document.createElementNS(svgNS, 'path'); path.classList.add('domain-link'); path.dataset.domainId = domain.id;
        path.setAttribute('d', `M480 357 Q${x * 10} 357 ${x * 10} ${y * 7}`); connections.append(path);
        const branch = document.createElementNS(svgNS, 'path'); branch.classList.add('record-link'); branch.setAttribute('d', `M${x * 10} ${y * 7 + 35} L${x * 10} ${(y + 15) * 7}`); connections.append(branch);
        const row = document.createElement('button'); row.dataset.domainId = domain.id; row.textContent = `${domain.label} · ${domain.priority} — ${record.summary}`; row.addEventListener('click', () => onOpen(record)); list.append(row);
      }
    }
    const selected = fixture.records.find(r => r.id === state.selected)?.domain;
    orbit.querySelectorAll('[data-domain-id]').forEach(n => n.classList.toggle('selected', n.dataset.domainId === selected));
    list.querySelectorAll('[data-domain-id]').forEach(n => n.setAttribute('aria-pressed', String(n.dataset.domainId === selected)));
  }
  return { render };
}
