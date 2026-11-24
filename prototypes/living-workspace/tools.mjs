// Registered workspace tools: renderer per store tool id. Renderers receive a
// record and a narrow api (select, investigate, askMax, lookups); they never
// dispatch directly or touch other panels.
import { tools } from './store.mjs';
import { pipelines } from './fixtures.mjs';
import * as ui from './surfaces.mjs';

const registry = new Map();
export function registerTool(id, renderer) {
  if (!Object.hasOwn(tools, id)) throw new Error(`Tool ${id} is not registered in the store`);
  registry.set(id, renderer);
}
export function renderTool(id, body, record, api) {
  const r = registry.get(id);
  if (!r) throw new Error(`No renderer for ${id}`);
  r(body, record, api);
}

const objectsOf = (api, domain) => api.fixture.records.filter(r => r.kind === 'object' && r.domain === domain);
const label = (api, domain) => api.fixture.overview.domains.find(d => d.id === domain).label;

registerTool('domain', (body, record, api) => {
  const investigate = ui.action('Investigate →', `Investigate ${record.name}`, () => api.investigate(record), 'primary');
  if (record.kind === 'object') {
    body.append(ui.header({ eyebrow: `${record.type} · ${label(api, record.domain)}`, title: record.name,
      chips: [ui.chip(record.stage, 'stage'), ui.statusChip(record.status)], lede: record.summary }));
    body.append(investigate);
    body.append(ui.section('Stage', ui.stageTrack(pipelines[record.type], record.stage)));
    body.append(ui.section(record.type === 'Prospect' ? 'Qualification signals' : 'Signals', ui.signalList(record.signals), { count: record.signals.length }));
    body.append(ui.section('Profile', ui.factGrid([...record.facts, ['Source', 'Local fixture']])));
    const related = record.related.map(rel => ({ rel, rec: api.recordFor(rel.recordId) })).filter(x => x.rec);
    body.append(ui.section('Relationships', related.map(({ rel, rec }) => ui.recordButton(rec, { relation: rel.relation, onSelect: () => api.select(rec) })), { count: related.length, empty: 'No related records.' }));
    body.append(ui.section('Suggested next actions', ui.actionList(record.nextActions, a => api.runAction(record, a))));
    const siblings = objectsOf(api, record.domain);
    if (siblings.length > 1) body.append(ui.section(`Also in ${label(api, record.domain)}`, siblings.map(o => ui.recordButton(o, { current: o.id === record.id, onSelect: () => api.select(o) })), { count: siblings.length }));
    return;
  }
  const objects = objectsOf(api, record.domain);
  const domain = api.fixture.overview.domains.find(d => d.id === record.domain);
  body.append(ui.header({ eyebrow: `Domain · ${domain.label}`, title: record.name, chips: [ui.statusChip(domain.status)], lede: record.summary }));
  body.append(investigate);
  body.append(ui.metricRow([[domain.objectPlural, objects.length], ['need attention', objects.filter(o => o.status === 'attention').length], ['open questions', objects.reduce((n, o) => n + o.questions.length, 0)]]));
  body.append(ui.section('In this domain', objects.map(o => ui.recordButton(o, { onSelect: () => api.select(o) })), { count: objects.length }));
  body.append(ui.section('Record', ui.factGrid([['Record', record.id], ['Status', domain.note], ['Source', 'Local fixture']])));
});

registerTool('investigation', (body, record, api) => {
  if (record.kind === 'object') {
    body.append(ui.header({ eyebrow: `Investigation · ${record.type}`, title: record.name, chips: [ui.chip(record.stage, 'stage'), ui.statusChip(record.status)], lede: record.summary }));
    body.append(ui.section('Evidence', ui.evidenceList(record.evidence), { count: record.evidence.length }));
    body.append(ui.section('Research findings', ui.findingList(record.findings), { count: record.findings.length, empty: 'No findings yet.' }));
    body.append(ui.section('Outstanding questions', ui.questionList(record.questions), { count: record.questions.length, empty: 'No open questions.' }));
    body.append(ui.section('Activity', ui.timeline([...record.activity].reverse()), { count: record.activity.length }));
  } else {
    const objects = objectsOf(api, record.domain);
    body.append(ui.header({ eyebrow: 'Investigation · Domain', title: record.name, lede: record.summary }));
    body.append(ui.section('Evidence', ui.evidenceList(record.evidence.map(t => ({ text: (s => s[0].toUpperCase() + s.slice(1))(t.replace(/^Fixture note: /, '')), source: 'Fixture note', observed: '2026-11-15', confidence: 'medium' }))), { count: record.evidence.length }));
    const questions = objects.flatMap(o => o.questions.map(q => `${o.name}: ${q}`));
    body.append(ui.section('Outstanding questions', ui.questionList(questions), { count: questions.length, empty: 'No open questions.' }));
    const activity = objects.flatMap(o => o.activity.map(a => ({ ...a, text: `${o.name} — ${a.text}` }))).sort((a, b) => b.at.localeCompare(a.at)).slice(0, 6);
    body.append(ui.section('Recent activity', ui.timeline(activity), { count: activity.length }));
  }
  body.append(ui.el('p', 'Fictional fixture data. Nothing here contacts anyone or runs anything.', 'provenance-note'));
  body.append(ui.action('Select this record', `Select ${record.name}`, () => api.select(record), 'quiet'));
});
