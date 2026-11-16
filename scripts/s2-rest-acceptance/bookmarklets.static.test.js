'use strict';
/*
 * Static + execution checks over the EXACT built "javascript:" strings.
 *   node --test scripts/s2-rest-acceptance/bookmarklets.static.test.js
 */
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { buildAll } = require('./buildBookmarklet');

const FILES = ['bookmarklet.txt', 'diagnostic.bookmarklet.txt', 'sizeprobe.bookmarklet.txt'];
const read = (f) => fs.readFileSync(path.join(__dirname, f), 'utf8');
const decode = (u) => decodeURIComponent(u.trim().slice('javascript:'.length));

test('committed bookmarklets match a fresh build byte for byte', () => {
  const built = buildAll();
  for (const f of FILES) assert.strictEqual(read(f), built[f] + '\n', f + ' is stale; rebuild');
});

for (const f of FILES) {
  test(f + ': one line, safe charset, end marker, parses', () => {
    const raw = read(f);
    assert.strictEqual(raw.split('\n').length, 2);
    const url = raw.trim();
    assert.match(url, /^javascript:[A-Za-z0-9%'()*!~._-]+$/);
    assert.ok(url.endsWith("void%20'S2_END_OF_BOOKMARKLET'%3B"));
    assert.ok(!/%(?![0-9A-F]{2})/.test(url), 'malformed percent escape');
    const code = decode(url);
    assert.doesNotThrow(() => new vm.Script(code));
  });
}

test('truncating any bookmarklet makes it unparseable (silent in a browser)', () => {
  for (const f of FILES) {
    const code = decode(read(f));
    const cut = code.slice(0, Math.floor(code.length * 0.9));
    assert.throws(() => new vm.Script(cut), SyntaxError, f);
  }
});

function runDiagnostic(file) {
  const calls = [];
  const deny = (name) => () => { calls.push(name); throw new Error('network API used: ' + name); };
  let appended = null;
  const ctx = {
    navigator: { userAgent: 'Mozilla/5.0 (Linux; Android 14) Chrome/131.0.0.0 Safari/537.36', sendBeacon: deny('sendBeacon') },
    location: { origin: 'https://pulseforge-leadgen-production.up.railway.app', pathname: '/dashboard',
      assign: deny('location.assign'), replace: deny('location.replace') },
    document: {
      readyState: 'complete',
      getElementById: () => null,
      createElement: (tag) => { if (tag !== 'div') calls.push('createElement:' + tag); return { style: {}, tagName: tag }; },
      body: { appendChild: (el) => { appended = el; } },
    },
    fetch: deny('fetch'), XMLHttpRequest: deny('XMLHttpRequest'), WebSocket: deny('WebSocket'), Image: deny('Image'),
    EventSource: deny('EventSource'), alert: deny('alert'), confirm: deny('confirm'),
  };
  const result = vm.runInNewContext(decode(read(file)), ctx);
  return { calls, appended, result };
}

test('diagnostic: shows the startup message, zero network APIs, completion undefined', () => {
  const r = runDiagnostic('diagnostic.bookmarklet.txt');
  assert.deepStrictEqual(r.calls, []);
  assert.strictEqual(r.result, undefined);
  assert.match(r.appended.textContent, /^S2 DIAGNOSTIC: bookmarklet RAN \(build [0-9a-f]{12}\)/);
  assert.match(r.appended.textContent, /kind: minimal {2}length: \d+ chars/);
  assert.match(r.appended.textContent, /page: https:\/\/pulseforge-leadgen-production\.up\.railway\.app\/dashboard/);
  assert.match(r.appended.textContent, /chrome: 131/);
  const len = read('diagnostic.bookmarklet.txt').trim().length;
  assert.ok(r.appended.textContent.includes('length: ' + len + ' chars'));
});

test('size probe: same behaviour, at least as long as the acceptance bookmarklet', () => {
  const r = runDiagnostic('sizeprobe.bookmarklet.txt');
  assert.deepStrictEqual(r.calls, []);
  assert.strictEqual(r.result, undefined);
  const len = read('sizeprobe.bookmarklet.txt').trim().length;
  assert.ok(len >= read('bookmarklet.txt').trim().length);
  assert.ok(r.appended.textContent.includes('kind: size-probe  length: ' + len + ' chars'));
});

test('diagnostic source contains no network API, no REST path, no intent', () => {
  for (const f of ['diagnostic.bookmarklet.txt', 'sizeprobe.bookmarklet.txt']) {
    const code = decode(read(f));
    for (const bad of ['fetch', 'XMLHttpRequest', 'sendBeacon', 'WebSocket', 'Image', 'EventSource', '/api/', 'cookie',
      'localStorage', 'sessionStorage', '.submit', 'location.href', 'location.assign', 'location.replace', 'open(',
      'EXECUTE_OUTBOUND', 'APPROVE_EXECUTION', 'GENERATE_VARIANTS', 'ASSESS_PAID_ACQUISITION', 'START_DISCOVERY',
      'APPROVE_DISCOVERY', 'intent', 'mission_']) {
      assert.ok(!code.includes(bad), f + ' contains ' + bad);
    }
  }
});

test('acceptance: completion value is undefined (page is not replaced)', () => {
  const code = decode(read('bookmarklet.txt'));
  const ctx = {
    window: {}, location: { origin: 'https://wrong.example' }, alert() {}, confirm: () => false,
    document: { createElement: () => ({ style: {} }), body: { appendChild() {} } },
    fetch: () => { throw new Error('no fetch expected on wrong origin'); },
  };
  ctx.window = ctx;
  assert.strictEqual(vm.runInNewContext(code, ctx), undefined);
});
