const assert = require('node:assert/strict');
const test = require('node:test');

const dbPath = require.resolve('../db');
require.cache[dbPath] = {
  id: dbPath,
  filename: dbPath,
  loaded: true,
  exports: { query: async () => ({ rows: [] }) },
};

const {
  LIVE_WORKSTREAMS,
  PROJECTS,
  JACOB_CONTEXT,
  isActiveTodoistContextItem,
} = require('../utils/miraWorld');
const { rankTodoistTasks } = require('../utils/miraAnchor');
const { isDigestWindow } = require('../miraDigestAgent');

test('Mira has exactly the three current workstreams and distinct markets', () => {
  assert.deepEqual(LIVE_WORKSTREAMS.map(row => row.name), ['Pulseforge', 'Anchor Cleaning', 'Upwork']);
  assert.match(PROJECTS.join('\n'), /Pulseforge.*Providence/i);
  assert.match(PROJECTS.join('\n'), /Anchor Cleaning.*Manchester/i);
  assert.doesNotMatch(PROJECTS.join('\n'), /MSHI|Nashville|Jacob Unbound/i);
  assert.doesNotMatch(JACOB_CONTEXT, /MSHI|Nashville/i);
});

test('Todoist context admits fresh Anchor work and rejects parked, alert, and dead-client tasks', () => {
  assert.equal(isActiveTodoistContextItem('Anchor Outreach', 'Call the next CPA'), true);
  assert.equal(isActiveTodoistContextItem('Jacob Unbound', 'Write a post'), false);
  assert.equal(isActiveTodoistContextItem('Pulseforge Stabilization Sprint', 'Run a setup test'), false);
  assert.equal(isActiveTodoistContextItem('Pulseforge', 'Alert: Scout produces 0 leads'), false);
  assert.equal(isActiveTodoistContextItem('Pulseforge', 'Call Brad about MSHI'), false);

  const projects = new Map([
    ['anchor', 'Anchor Outreach'],
    ['pulseforge', 'Pulseforge'],
    ['parked', 'Jacob Unbound'],
  ]);
  const ranked = rankTodoistTasks([
    { id: 'old', project_id: 'pulseforge', content: 'Review Providence campaign', added_at: '2026-06-01T12:00:00Z' },
    { id: 'fresh', project_id: 'anchor', content: 'Call the next Manchester CPA', added_at: '2026-06-30T12:00:00Z' },
    { id: 'dead', project_id: 'pulseforge', content: 'MSHI deliverability follow-up', added_at: '2026-06-30T12:00:00Z' },
    { id: 'resting', project_id: 'parked', content: 'Write Jacob Unbound thread', added_at: '2026-06-30T12:00:00Z' },
  ], projects, new Date('2026-06-30T16:00:00Z'));

  assert.deepEqual(ranked.map(row => row.id), ['fresh', 'old']);
});

test('scheduled digest window is 7:00-7:14 AM Eastern, not 6:30', () => {
  assert.equal(isDigestWindow(new Date('2026-06-30T10:30:00Z')), false);
  assert.equal(isDigestWindow(new Date('2026-06-30T11:00:00Z')), true);
  assert.equal(isDigestWindow(new Date('2026-06-30T11:14:59Z')), true);
  assert.equal(isDigestWindow(new Date('2026-06-30T11:15:00Z')), false);
});
