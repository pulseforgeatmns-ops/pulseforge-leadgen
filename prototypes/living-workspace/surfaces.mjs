// Reusable, dependency-free surface components for registered workspace tools.
// Pure DOM builders: no state, no dispatch. Future agent tools (Scout, Emmett,
// Paige, Penny, Rex) compose the same pieces with their own fixture data.
export function el(tag, text, className, attrs = {}) {
  const n = document.createElement(tag);
  if (text) n.textContent = text;
  if (className) n.className = className;
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  return n;
}
export function action(text, label, onClick, className = '') {
  const b = el('button', text, className, { type: 'button', 'aria-label': label });
  b.addEventListener('click', onClick); return b;
}
const STATUS = { attention: 'Needs attention', steady: 'On track', new: 'New', active: 'Active' };
export function statusChip(status) {
  const c = el('span', '', `status-chip status-${status}`);
  c.append(el('i', '', 'status-dot', { 'aria-hidden': 'true' }), document.createTextNode(STATUS[status] || status));
  return c;
}
export const chip = (text, tone = '') => el('span', text, `chip ${tone}`.trim());
export function header({ eyebrow, title, chips = [], lede }) {
  const h = el('div', '', 'surface-head');
  h.append(el('p', eyebrow, 'eyebrow'), el('h2', title));
  if (chips.length) { const row = el('div', '', 'chip-row'); row.append(...chips); h.append(row); }
  if (lede) h.append(el('p', lede, 'lede'));
  return h;
}
export function section(title, content, { count, empty } = {}) {
  const s = el('section', '', 'surface-section');
  const h = el('h3', title);
  if (count !== undefined) h.append(el('span', String(count), 'count'));
  s.append(h);
  const items = [].concat(content).filter(Boolean);
  s.append(...(items.length ? items : [el('p', empty || 'Nothing recorded.', 'empty')]));
  return s;
}
export function stageTrack(stages, current) {
  const ol = el('ol', '', 'stage-track', { 'aria-label': `Stage: ${current} of ${stages.length}` });
  const at = stages.indexOf(current);
  stages.forEach((name, i) => {
    const li = el('li', name, i < at ? 'done' : i === at ? 'current' : 'todo');
    if (i === at) li.setAttribute('aria-current', 'step');
    ol.append(li);
  });
  return ol;
}
const STRENGTH = { strong: 3, moderate: 2, weak: 1 };
export function signalList(signals) {
  return signals.map(s => {
    const row = el('div', '', 'signal');
    const meter = el('span', '', `meter meter-${s.strength}`, { role: 'img', 'aria-label': `${s.strength} signal` });
    for (let i = 1; i <= 3; i++) meter.append(el('i', '', i <= STRENGTH[s.strength] ? 'on' : ''));
    const text = el('div'); text.append(el('strong', s.label), el('span', s.note, 'note'));
    row.append(meter, text); return row;
  });
}
export function factGrid(rows) {
  const dl = el('dl', '', 'fact-grid');
  for (const [k, v] of rows) dl.append(el('dt', k), el('dd', v));
  return dl;
}
export function recordButton(record, { relation, onSelect, current }) {
  const b = action('', `Select ${record.name}`, onSelect, 'record-row');
  if (current) b.setAttribute('aria-current', 'true');
  const main = el('span', '', 'record-main');
  if (relation) main.append(el('span', relation, 'relation'));
  main.append(el('strong', record.name), el('span', record.kind === 'object' ? `${record.type} · ${record.stage}` : 'Domain', 'meta'));
  b.append(el('i', '', `status-dot status-${record.status || 'steady'}`, { 'aria-hidden': 'true' }), main);
  return b;
}
// Next actions are suggestions. Workspace actions change the view; proposals are labelled and never run.
export function actionList(actions, run) {
  return actions.map(a => {
    if (a.kind === 'proposal') {
      const row = el('div', '', 'next-action proposal');
      row.append(el('span', a.label), chip('Proposal · not executed', 'muted'));
      return row;
    }
    const b = action(a.label, a.label, () => run(a), 'next-action');
    b.append(el('span', a.kind === 'max' ? 'Ask Max' : 'Open', 'go'));
    return b;
  });
}
export function evidenceList(items) {
  return items.map((e, i) => {
    const row = el('article', '', 'evidence');
    row.append(el('span', String(i + 1).padStart(2, '0'), 'index'));
    const body = el('div');
    const prov = el('p', '', 'provenance');
    prov.append(el('span', e.source), el('span', e.observed), chip(`${e.confidence} confidence`, `confidence-${e.confidence}`));
    body.append(el('p', e.text), prov);
    row.append(body); return row;
  });
}
export function findingList(items) {
  return items.map(f => { const p = el('p', '', 'finding'); p.append(el('span', f.text), chip(f.confidence, `confidence-${f.confidence}`)); return p; });
}
export function timeline(items) {
  const ol = el('ol', '', 'timeline');
  for (const a of items) {
    const li = el('li');
    li.append(el('time', a.at), el('span', a.actor, 'actor'), el('span', a.text, 'what'));
    ol.append(li);
  }
  return ol;
}
export function questionList(questions) {
  return questions.map(q => { const p = el('p', '', 'question'); p.append(el('span', '?', 'mark', { 'aria-hidden': 'true' }), el('span', q)); return p; });
}
export function metricRow(metrics) {
  const row = el('div', '', 'metric-row');
  for (const [label, value] of metrics) { const m = el('div', '', 'metric'); m.append(el('strong', String(value)), el('span', label)); row.append(m); }
  return row;
}
