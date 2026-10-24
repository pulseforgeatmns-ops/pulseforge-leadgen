'use strict';

// Phase A2 phone display + dial handoff continuity — always-run coverage.
//
// The server (utils/phone.js) and browser (public/shared/phone.js) modules
// must normalize identically, and the dial handoff controller must persist
// the active call workspace to sessionStorage before opening tel:.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const serverPhone = require('../utils/phone');

function loadBrowserPhoneModule() {
  const source = fs.readFileSync(
    path.join(__dirname, '..', 'public', 'shared', 'phone.js'),
    'utf8'
  );
  const storage = new Map();
  const sandbox = {
    sessionStorage: {
      getItem: key => (storage.has(key) ? storage.get(key) : null),
      setItem: (key, value) => storage.set(key, String(value)),
      removeItem: key => storage.delete(key),
    },
    document: {
      readyState: 'complete',
      visibilityState: 'visible',
      addEventListener: () => {},
      dispatchEvent: () => {},
      getElementById: () => null,
      createElement: () => ({ addEventListener: () => {}, setAttribute: () => {}, remove: () => {} }),
      body: { appendChild: () => {} },
    },
    navigator: { clipboard: { writeText: async () => {} } },
    setTimeout,
    clearTimeout,
    Date,
    JSON,
  };
  sandbox.window = {
    addEventListener: () => {},
    setTimeout,
    clearTimeout,
    location: { href: '', pathname: '/setter' },
  };
  vm.createContext(sandbox);
  vm.runInContext(source, sandbox);
  return { phone: sandbox.window.PulseforgePhone, sandbox, storage };
}

test('server phone utils normalize, format, and build tel: links', () => {
  assert.equal(serverPhone.normalizePhone('(603) 555-1234'), '+16035551234');
  assert.equal(serverPhone.normalizePhone('1-603-555-1234'), '+16035551234');
  assert.equal(serverPhone.normalizePhone('603.555.1234'), '+16035551234');
  assert.equal(serverPhone.normalizePhone('55512'), null);
  assert.equal(serverPhone.normalizePhone(''), null);
  assert.equal(serverPhone.formatPhoneDisplay('6035551234'), '(603) 555-1234');
  assert.equal(serverPhone.telHref('603-555-1234'), 'tel:+16035551234');
  const described = serverPhone.describePhone(' 603 555 1234 ');
  assert.equal(described.normalized, '+16035551234');
  assert.equal(described.display, '(603) 555-1234');
  assert.equal(described.callable, true);
  const missing = serverPhone.describePhone(null);
  assert.deepEqual(missing, { raw: null, normalized: null, display: null, callable: false });
});

test('browser phone module normalizes identically to the server module', () => {
  const { phone } = loadBrowserPhoneModule();
  for (const sample of [
    '(603) 555-1234', '1 603 555 1234', '603.555.1234', '55512', '', 'ext 44',
    '+1 (415) 555-0142',
  ]) {
    assert.equal(
      phone.normalizePhone(sample),
      serverPhone.normalizePhone(sample),
      `normalize drift for "${sample}"`
    );
    assert.equal(
      phone.telHref(sample),
      serverPhone.telHref(sample),
      `tel: drift for "${sample}"`
    );
  }
});

test('dial handoff persists the active call before navigating to tel:', () => {
  const { phone, sandbox, storage } = loadBrowserPhoneModule();
  const ok = phone.beginDialHandoff({
    prospectId: 'abc-123',
    phone: '(603) 555-1234',
    companyName: 'Granite Cleaning',
    clientId: 10,
    workspaceRoute: '/setter',
  });
  assert.equal(ok, true);
  assert.equal(sandbox.window.location.href, 'tel:+16035551234');
  const saved = JSON.parse(storage.get('pulseforge.activeCall'));
  assert.equal(saved.prospectId, 'abc-123');
  assert.equal(saved.companyName, 'Granite Cleaning');
  assert.equal(saved.clientId, 10);
  assert.equal(saved.workspaceRoute, '/setter');
  assert.ok(saved.startedAt, 'startedAt must be recorded for TTL expiry');

  // readActiveCall returns the persisted state; clearActiveCall removes it.
  const restored = phone.readActiveCall();
  assert.equal(restored.prospectId, 'abc-123');
  phone.clearActiveCall();
  assert.equal(phone.readActiveCall(), null);
});

test('dial handoff refuses to navigate without a dialable number', () => {
  const { phone, sandbox, storage } = loadBrowserPhoneModule();
  const ok = phone.beginDialHandoff({ prospectId: 'abc-123', phone: '' });
  assert.equal(ok, false);
  assert.equal(sandbox.window.location.href, '');
  assert.equal(storage.has('pulseforge.activeCall'), false);
});

test('stale active-call state expires after the TTL', () => {
  const { phone, storage } = loadBrowserPhoneModule();
  storage.set('pulseforge.activeCall', JSON.stringify({
    prospectId: 'old-1',
    startedAt: new Date(Date.now() - 5 * 60 * 60 * 1000).toISOString(),
  }));
  assert.equal(phone.readActiveCall(), null, 'calls older than 4 hours must not restore');
});

test('both HTML shells load the shared foundation and rename Phone Setter to Calls', () => {
  const root = path.join(__dirname, '..');
  const dashboard = fs.readFileSync(path.join(root, 'public', 'dashboard.html'), 'utf8');
  const setter = fs.readFileSync(path.join(root, 'public', 'setter-dashboard.html'), 'utf8');
  for (const html of [dashboard, setter]) {
    for (const asset of [
      '/shared/tokens.css', '/shared/shell.css', '/shared/accessibility.js',
      '/shared/api-client.js', '/shared/phone.js', '/shared/lifecycle.js',
      '/shared/prospect-workspace.js', '/shared/shell.js',
    ]) {
      assert.ok(html.includes(asset), `shell missing shared asset ${asset}`);
    }
  }
  // User-facing rename: Calls, with the /setter route preserved.
  assert.match(setter, /<title>Pulseforge — Calls<\/title>/);
  assert.doesNotMatch(setter, /SETTER DASHBOARD/);
  assert.match(dashboard, />Open Calls</);
  // Phone visibility: queue/pipeline rows render the shared phone cell.
  assert.match(setter, /function phoneCell\(/);
  assert.match(setter, /data-dial=/);
  // Server exposes the shared assets.
  const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  assert.match(server, /app\.use\('\/shared', express\.static/);
});
