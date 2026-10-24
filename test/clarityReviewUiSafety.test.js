'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const source = fs.readFileSync(path.join(__dirname, '../public/clarity-review.js'), 'utf8');
const livingSource = fs.readFileSync(path.join(__dirname, '../public/command-deck/living-command.js'), 'utf8');
const A = '00000000-0000-4000-8000-000000000001';
const B = '00000000-0000-4000-8000-000000000002';
const attack = '<img src=x onerror="globalThis.injected=true">';
const flush = () => new Promise(resolve => setImmediate(resolve));

class Element {
  constructor(tag = 'div') { this.tagName = tag; this.children = []; this.listeners = {}; this.dataset = {}; this.disabled = false; this.hidden = false; this.open = false; this._text = ''; }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
  set innerHTML(_) { throw new Error('Untrusted markup must not be parsed'); }
  append(...children) { this.children.push(...children); }
  appendChild(child) { this.append(child); return child; }
  replaceChildren(...children) { this._text = ''; this.children = children; }
  addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
  async emit(type, event = {}) { await Promise.all((this.listeners[type] || []).map(handler => handler(event))); }
  async click() { if (!this.disabled) await this.emit('click'); }
  setAttribute(name, value) { this[name] = value; }
  showModal() { this.open = true; }
  close() { this.open = false; }
  focus() {}
}

function fixture(id = A) {
  return {
    recommendation: { id, client_id: 10, digest: 'exact-hash', title: `Recommendation ${id} ${attack}`, summary: attack, status: 'pending', confidence: 'moderate', evidence: [{ label: attack, value: attack }], validation_steps: [attack], expected_outcome: attack },
    report: { synthetic: true, window_start: '2026-10-12', window_end: '2026-10-18', metrics: { sessions: 3, scroll_depth: 20, rage_clicks: null }, deltas_pct: { sessions: 2 }, provenance: { gmail_message_id: attack } },
    permissions: { can_decide: true }, csrf_token: 'csrf-exact',
  };
}
const response = (data, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => structuredClone(data) });

function setup({ fetch, search = `?id=${A}&client_id=10`, script = source, activeTenant = 10 } = {}) {
  const elements = new Map();
  const document = new Element();
  document.getElementById = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
  document.createElement = tag => new Element(tag);
  document.querySelectorAll = () => [];
  const window = new Element();
  window.PulseforgeShell = { activeClientId: activeTenant };
  const location = { search };
  const calls = [];
  const context = { window, document, location, URLSearchParams, AbortController, crypto, console, fetch: async (...args) => { calls.push(args); return fetch ? fetch(...args) : response(fixture()); } };
  vm.runInNewContext(script, context);
  return { el: id => document.getElementById(id), window, document, location, calls, context };
}

test('Clarity UI uses text-only render for every untrusted field and preserves missing metrics', async () => {
  const { el, context } = setup();
  await flush();
  assert.match(el('clarityContent').textContent, /Synthetic test data/);
  assert.ok(el('clarityContent').textContent.includes(attack));
  assert.match(el('clarityContent').textContent, /Rage clicksUnavailable/);
  assert.match(el('clarityContent').textContent, /Scroll depth20%/);
  assert.match(el('clarityContent').textContent, /Reported change: \+2%/);
  assert.equal(context.injected, undefined);
  assert.equal(el('clarityActions').hidden, false);
});

test('Clarity review cancel makes no write; duplicate confirm is locked and CSRF/digest/client/key are bound', async () => {
  let finish;
  const s = setup({ fetch: (url, options) => options.method === 'POST' ? new Promise(resolve => { finish = resolve; }) : response(fixture()) });
  await flush();
  await s.el('clarityApprove').click();
  assert.equal(s.el('clarityConfirm').open, true);
  assert.match(s.el('clarityConfirmCopy').textContent, /does not authorize implementation/);
  await s.el('clarityCancel').click();
  assert.equal(s.calls.length, 1);
  await s.el('clarityApprove').click();
  const first = s.el('clarityConfirmSubmit').click();
  await s.el('clarityConfirmSubmit').click();
  assert.equal(s.calls.length, 2);
  let prevented = false;
  await s.el('clarityConfirm').emit('cancel', { preventDefault() { prevented = true; } });
  assert.equal(prevented, true);
  const [url, options] = s.calls[1];
  assert.equal(url, `/api/v1/clarity/recommendations/${A}/decision`);
  assert.equal(options.headers['X-CSRF-Token'], 'csrf-exact');
  const body = JSON.parse(options.body);
  assert.equal(body.expected_digest, 'exact-hash');
  assert.equal(body.client_id, 10);
  assert.equal(body.decision, 'approve');
  assert.match(body.idempotency_key, /^[a-f0-9-]{36}$/);
  finish(response({ recommendation: { ...fixture().recommendation, status: 'approved' }, decision: { website_changes_authorized: false } }));
  await first;
  assert.equal(s.el('clarityActions').hidden, true);
  assert.match(s.el('clarityStatus').textContent, /No website changes have been authorized/);
  assert.equal(s.el('clarityConfirm').open, false);
});

test('uncertain decision requires reconciliation and preserves same idempotency key on retry', async () => {
  let attempts = 0;
  const s = setup({ fetch: (url, options) => {
    if (options.method !== 'POST') return response(fixture());
    attempts += 1;
    if (attempts === 1) throw new Error('network disconnected');
    return response({ recommendation: { ...fixture().recommendation, status: 'rejected' }, decision: { website_changes_authorized: false } });
  } });
  await flush();
  await s.el('clarityReject').click();
  await s.el('clarityConfirmSubmit').click();
  assert.match(s.el('clarityStatus').textContent, /could not be confirmed/);
  assert.equal(s.el('clarityReject').disabled, true);
  await s.el('clarityReload').click();
  assert.equal(s.el('clarityReject').disabled, false);
  assert.equal(s.el('clarityApprove').disabled, true);
  await s.el('clarityReject').click();
  await s.el('clarityConfirmSubmit').click();
  const writes = s.calls.filter(([, opts]) => opts.method === 'POST');
  assert.equal(writes.length, 2);
  assert.equal(writes[0][1].body, writes[1][1].body);
  assert.match(s.el('clarityStatus').textContent, /Recommendation rejected/);
});

test('later navigation invalidates delayed GET and tenant changes clear evidence and dialog', async () => {
  let finishOld;
  const s = setup({ fetch: url => url.includes(A) ? new Promise(resolve => { finishOld = resolve; }) : response(fixture(B)) });
  s.location.search = `?id=${B}&client_id=10`;
  await s.window.emit('popstate');
  finishOld(response(fixture(A)));
  await flush();
  assert.match(s.el('clarityTitle').textContent, new RegExp(B));
  assert.equal(s.calls[0][1].signal.aborted, true);
  await s.el('clarityApprove').click();
  assert.equal(s.el('clarityConfirm').open, true);
  await s.document.emit('pulseforge:tenant-changed');
  assert.equal(s.el('clarityContent').textContent, '');
  assert.equal(s.el('clarityConfirm').open, false);
  assert.equal(s.el('clarityActions').hidden, true);
  assert.equal(s.el('clarityReload').disabled, true);
});

test('a late decision result cannot replace a newer recommendation after back/forward navigation', async () => {
  let finish;
  const s = setup({ fetch: (url, opts) => opts.method === 'POST'
    ? new Promise(resolve => { finish = resolve; }) : response(fixture(url.includes(B) ? B : A)) });
  await flush();
  await s.el('clarityApprove').click();
  const decision = s.el('clarityConfirmSubmit').click();
  s.location.search = `?id=${B}&client_id=10`;
  await s.window.emit('popstate');
  finish(response({ recommendation: { ...fixture(A).recommendation, status: 'approved' }, decision: { website_changes_authorized: false } }));
  await decision;
  assert.match(s.el('clarityTitle').textContent, new RegExp(B));
  assert.doesNotMatch(s.el('clarityStatus').textContent, /Recommendation approved/);
  assert.equal(s.el('clarityConfirm').open, false);
  assert.equal(s.el('clarityApprove').disabled, false);
});

test('invalid links never request data; role, CSRF and response tenant mismatches fail closed', async () => {
  for (const search of [`?id=${A}`, `?id=${A}&client_id=0`, `?id=${A}&client_id=10&client_id=11`, '?id=javascript:alert(1)&client_id=10']) {
    const s = setup({ search });
    await flush();
    assert.equal(s.calls.length, 0);
    assert.equal(s.el('clarityActions').hidden, true);
  }
  for (const mode of ['tenant', 'role', 'csrf', 'digest']) {
    const data = fixture();
    if (mode === 'tenant') data.recommendation.client_id = 11;
    if (mode === 'role') data.permissions.can_decide = false;
    if (mode === 'csrf') delete data.csrf_token;
    if (mode === 'digest') delete data.recommendation.digest;
    const s = setup({ fetch: () => response(data) });
    await flush();
    assert.equal(s.el('clarityActions').hidden, true);
    await s.el('clarityApprove').click();
    assert.equal(s.el('clarityConfirm').open, false);
    assert.equal(s.calls.length, 1);
    if (mode === 'tenant') assert.equal(s.el('clarityContent').textContent, '');
  }
});

test('stale hash conflict and unauthorized responses never render raw provider errors', async () => {
  for (const code of [401, 403, 404, 503]) {
    const s = setup({ fetch: () => response({ error: 'secret_raw_error' }, code) });
    await flush();
    assert.doesNotMatch(s.el('clarityStatus').textContent, /secret_raw_error/);
    assert.equal(s.el('clarityActions').hidden, true);
  }
  const s = setup({ fetch: (url, opts) => response(opts.method === 'POST' ? { error: 'secret_raw_error' } : fixture(), opts.method === 'POST' ? 409 : 200) });
  await flush();
  await s.el('clarityApprove').click();
  await s.el('clarityConfirmSubmit').click();
  assert.match(s.el('clarityStatus').textContent, /changed or was already reviewed/);
  assert.equal(s.el('clarityApprove').disabled, true);
  assert.equal(s.el('clarityReload').disabled, false);
});

test('daily brief renders exact server order, bounded tenant links and review-only language', () => {
  const s = setup({ script: livingSource });
  const recs = [fixture(B).recommendation, fixture(A).recommendation];
  s.window.LivingCommand.render({ livingBrief: { clientId: 10, clarity: { available: true, report: fixture().report, recommendations: recs } } });
  const section = s.el('lcBriefLines').children.at(-1);
  const cards = section.children.filter(child => child.className === 'lc-clarity-card');
  assert.equal(cards.length, 2);
  assert.match(cards[0].href, new RegExp(B));
  assert.match(cards[1].href, new RegExp(A));
  assert.match(cards[0].href, /client_id=10/);
  assert.ok(section.textContent.includes(attack));
  assert.match(section.textContent, /Website changes require separate approval/);
  for (const recommendations of [[...recs, ...recs], [{ ...recs[0], client_id: 11 }]]) {
    s.window.LivingCommand.render({ livingBrief: { clientId: 10, clarity: { available: true, recommendations } } });
    assert.equal(s.el('lcBriefLines').children.at(-1).children.filter(child => child.className === 'lc-clarity-card').length, 0);
  }
  s.window.LivingCommand.clear();
  assert.equal(s.el('lcBriefLines').children.length, 0);
});

test('dark, unconsented and other-tenant Clarity surfaces are absent; late shell identity is required', async () => {
  const s = setup({ script: livingSource });
  const model = { livingBrief: { clientId: 10, clarity: { available: true, report: fixture().report, recommendations: [fixture().recommendation] } } };
  const section = () => s.el('lcBriefLines').children.find(child => child.className === 'lc-clarity');
  for (const reason of ['disabled', 'not_authorized', 'authorization_required']) {
    s.window.LivingCommand.render({ livingBrief: { ...model.livingBrief, clarity: { available: false, reason } } });
    assert.equal(section(), undefined);
  }
  for (const activeClientId of [11, null]) {
    s.window.PulseforgeShell.activeClientId = activeClientId;
    s.window.LivingCommand.render(model);
    assert.equal(section(), undefined);
  }
  s.window.PulseforgeShell.activeClientId = 10;
  await s.document.emit('pulseforge:shell-ready');
  assert.ok(section());
  s.window.PulseforgeTenantCoordinator = { getActiveClientId: () => 11 };
  await s.document.emit('pulseforge:tenant-changed');
  assert.equal(s.el('lcBriefLines').children.length, 0);
  // Even if an older response reaches the renderer directly, coordinator wins
  // over the stale shell and server brief's internally consistent tenant IDs.
  s.window.LivingCommand.render(model);
  assert.equal(section(), undefined);
});

function deckHarness() {
  const code = fs.readFileSync(path.join(__dirname, '../public/command-deck/command-deck.js'), 'utf8');
  function part(start, end) {
    const from = code.indexOf(start), to = code.indexOf(end, from);
    assert.ok(from >= 0 && to > from);
    return code.slice(from, to);
  }
  const storage = new Map(), renders = [], writes = [], failures = [], requests = [];
  const context = vm.createContext({
    window: { PulseforgeShell: { activeClientId: 10 } }, document: { hidden: false }, console: { warn() {}, error() {} },
    sessionStorage: { getItem: key => storage.get(key), setItem: (key, value) => storage.set(key, value) },
    tenantIdFromModel: model => model?.meta?.tenantId ?? null,
    setStatus() {}, hideError() {}, clearSections() {},
    renderModel: model => renders.push(model), evolveRender: model => renders.push(model),
    showError: () => failures.push(vm.runInContext('readCache()', context)),
    fetchCommandDeck: () => new Promise((resolve, reject) => requests.push({ resolve, reject })),
    apiRequest: () => new Promise((resolve, reject) => requests.push({ resolve, reject })),
    showLiveNotify: notice => writes.push(notice), appendEvolutionFootnotes() {},
    URLSearchParams, els: { workspace: null },
  });
  vm.runInContext(`const CACHE_KEY = 'deck'; let currentModel = null, deckLoadGeneration = 0, liveCursor = '', livePollInFlight = false;
    let workspaceSessionId = null, workspaceContext = null, investigation = null;
    ${part('  function activeDeckTenantId()', '  async function fetchCommandDeck()')}
    ${part('  async function softEvolve()', '  function evolveRender(')}
    ${part('  async function loadDeck(', "  els.askForm.addEventListener")}
    ${part('  function invalidateWorkspaceForTenantChange()', '  async function onTenantChanged()')}`, context);
  return { storage, renders, writes, failures, requests, context, run: expression => vm.runInContext(expression, context) };
}

test('command deck strips Clarity from persisted and legacy cache; mismatched or unknown tenant cannot restore cache', () => {
  const h = deckHarness();
  h.context.model = { meta: { tenantId: '10' }, livingBrief: { clientId: 10, clarity: { report: { secret: 'tenant evidence' } }, pipeline: { total: 8 } } };
  h.run('writeCache(model)');
  assert.equal(JSON.parse(h.storage.get('deck')).model.livingBrief.clarity, undefined);
  assert.ok(h.context.model.livingBrief.clarity, 'cache stripping must not mutate live model');
  h.storage.set('deck', JSON.stringify({ model: h.context.model }));
  assert.equal(h.run('readCache().model.livingBrief.clarity'), undefined);
  assert.equal(JSON.parse(h.storage.get('deck')).model.livingBrief.clarity, undefined);
  h.context.window.PulseforgeShell.activeClientId = 11;
  assert.equal(h.run('readCache()'), null);
  h.context.window.PulseforgeShell.activeClientId = null;
  assert.equal(h.run('readCache()'), null);
});

test('old deck fetch cannot paint or persist after tenant change; new tenant failure cannot offer old cache', async () => {
  const h = deckHarness();
  const oldModel = { meta: { tenantId: '10' }, livingBrief: { clientId: 10, clarity: fixture() } };
  h.storage.set('deck', JSON.stringify({ model: oldModel }));
  const oldLoad = h.run('loadDeck()');
  h.context.window.PulseforgeShell.activeClientId = 11;
  h.run('invalidateWorkspaceForTenantChange()');
  const newLoad = h.run('loadDeck()');
  h.requests[1].reject(new Error('new workspace unavailable'));
  await newLoad;
  h.requests[0].resolve(oldModel);
  await oldLoad;
  assert.deepEqual(h.renders, []);
  assert.deepEqual(h.failures, [null]);
  assert.equal(h.run('currentModel'), null);
  assert.equal(JSON.parse(h.storage.get('deck')).model.livingBrief.clarity, undefined);
});

test('live poll response and notifications from previous tenant are fenced before any paint', async () => {
  const h = deckHarness();
  const poll = h.run('softEvolve()');
  h.context.window.PulseforgeShell.activeClientId = 11;
  h.run('invalidateWorkspaceForTenantChange()');
  h.requests[0].resolve({ cursor: 'old-tenant-cursor', notifications: [{ summary: 'old private notice' }], deck: { meta: { tenantId: '10' }, livingBrief: { clientId: 10, clarity: fixture() } }, hasUpdates: true });
  await poll;
  assert.deepEqual(h.renders, []);
  assert.deepEqual(h.writes, []);
  assert.equal(h.storage.size, 0);
  assert.equal(h.run('liveCursor'), '');
  assert.equal(h.run('livePollInFlight'), false);
});
