'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync, existsSync } = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.join(__dirname, '..');
const read = file => readFileSync(path.join(root, file), 'utf8');
const html = read('pulseforge-leadgen.html');
const scripts = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)].map(match => match[1]);
const lead = (id, email = 'owner@example.test', score = 80) => ({
  id, email, score, company: `Fixture ${id}`, url: 'example.test', contact: 'Owner',
  title: 'Owner', type: 'smb', sources: ['google'],
});

function harness({ constructorFails = false } = {}) {
  const elements = new Map();
  const streams = [];
  function element() {
    const classes = new Set();
    return {
      textContent: '', innerHTML: '', value: '', disabled: false, style: {}, children: [],
      classList: {
        toggle(name, force) {
          const add = force === undefined ? !classes.has(name) : force;
          if (add) classes.add(name); else classes.delete(name);
        },
        contains: name => classes.has(name),
      },
      appendChild(child) { this.children.push(child); },
    };
  }
  for (const [, id] of html.matchAll(/\bid="([^"]+)"/g)) elements.set(id, element());
  elements.get('max-results').value = '10';
  elements.get('min-score').value = '0';
  class EventSource {
    constructor(url) {
      if (constructorFails) throw new Error('fixture connection unavailable');
      this.url = url;
      this.closed = false;
      streams.push(this);
    }
    close() { this.closed = true; }
    results(leads) { this.onmessage({ data: JSON.stringify({ type: 'results', leads }) }); }
  }
  // Execute the actual page script, but never expose a real network or provider API.
  const context = vm.createContext({
    URLSearchParams, EventSource,
    document: {
      getElementById(id) { assert.ok(elements.has(id), `Unknown element: ${id}`); return elements.get(id); },
      addEventListener() {},
      createElement: element,
    },
    fetch() { throw new Error('Network access is forbidden in this fixture'); },
  });
  for (const source of scripts) new vm.Script(source).runInContext(context);
  return { elements, streams, context, run: source => vm.runInContext(source, context) };
}

function assertEmpty(h) {
  assert.equal(h.run('state.leads.length'), 0);
  assert.equal(h.elements.get('stat-total').textContent, 0);
  assert.equal(h.elements.get('stat-emails').textContent, 0);
  assert.equal(h.elements.get('stat-high').textContent, 0);
  assert.equal(h.elements.get('stat-emails-d').textContent, '0% hit rate');
  assert.match(h.elements.get('leads-tbody').innerHTML, /No leads match current filter/);
  assert.equal(h.run('state.running'), false);
  assert.equal(h.elements.get('run-btn').disabled, false);
  assert.equal(h.elements.get('run-btn').textContent, '▶ Run Search');
  assert.equal(h.elements.get('progress-wrap').classList.contains('hidden'), true);
}

test('empty lead statistics use a finite zero email hit rate', () => {
  const h = harness();
  h.run('updateStats([], 0)');
  for (const id of ['stat-total', 'stat-emails', 'stat-high', 'stat-dupes']) {
    assert.equal(h.elements.get(id).textContent, 0);
  }
  assert.equal(h.elements.get('stat-emails-d').textContent, '0% hit rate');
});

test('nonempty statistics retain email filtering, rounding and qualification counts', () => {
  const h = harness();
  h.context.leads = [lead(1), lead(2, '—', 40), lead(3, null, 70)];
  h.run('updateStats(leads, 4)');
  assert.equal(h.elements.get('stat-total').textContent, 3);
  assert.equal(h.elements.get('stat-emails').textContent, 1);
  assert.equal(h.elements.get('stat-emails-d').textContent, '33% hit rate');
  assert.equal(h.elements.get('stat-high').textContent, 2);
  assert.equal(h.elements.get('stat-dupes').textContent, 4);
  h.run("updateStats([{email: ''}, {}], 0)");
  assert.equal(h.elements.get('stat-emails-d').textContent, '0% hit rate');
});

test('zero-result searches clear previous results and restore controls', async () => {
  const h = harness();
  await h.run('runSearch()');
  h.streams[0].results([lead(1)]);
  assert.equal(h.elements.get('stat-emails-d').textContent, '100% hit rate');
  await h.run('runSearch()');
  h.streams[1].results([]);
  assertEmpty(h);
  assert.equal(h.elements.get('stat-dupes').textContent, 10);
  assert.ok(h.streams.every(stream => stream.closed));
});

test('EventSource construction failure clears stale results without synthetic fallback', async () => {
  const h = harness({ constructorFails: true });
  h.context.staleLeads = [lead(1)];
  h.run('state.leads = staleLeads; renderTable(); updateStats(staleLeads, 3)');
  await h.run('runSearch()');
  assertEmpty(h);
  assert.equal(h.elements.get('stat-dupes').textContent, 0);
  assert.match(h.elements.get('log-body').children.at(-1).innerHTML, /Could not connect to search API/);
});

test('stream errors show an empty state and allow retry without duplicate concurrent searches', async () => {
  const h = harness();
  await h.run('runSearch()');
  await h.run('runSearch()');
  assert.equal(h.streams.length, 1);
  h.streams[0].onerror({});
  assertEmpty(h);
  assert.equal(h.elements.get('stat-dupes').textContent, 0);
  assert.equal(h.streams[0].closed, true);
  assert.match(h.elements.get('log-body').children.at(-1).innerHTML, /Search API connection failed/);
  await h.run('runSearch()');
  h.streams[1].results([lead(2)]);
  h.streams[1].onerror({});
  assert.equal(h.run('state.leads.length'), 1, 'completed results survive a stream close error');
  assert.equal(h.elements.get('stat-emails-d').textContent, '100% hit rate');
  assert.equal(h.run('state.running'), false);
  assert.ok(h.streams.every(stream => stream.closed));
});

test('legacy dashboard demo route, data, banner and links remain removed', () => {
  assert.equal(existsSync(path.join(root, 'utils/demoData.js')), false);
  for (const file of ['AGENTS.md', 'CLAUDE.md', 'server.js', 'public/dashboard.html', 'public/setter-dashboard.html', 'pulseforge-leadgen.html']) {
    assert.doesNotMatch(read(file), /generateDemoData|DEMO_MODE|DEMO_DATA|demo-banner|demoBanner|mockLeads|useDemoData|\/demo(?:['"`\s<]|$)/, file);
  }
});
