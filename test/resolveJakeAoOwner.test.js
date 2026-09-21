'use strict';

const assert = require('node:assert/strict');
const path = require('node:path');
const test = require('node:test');

const CLIENT_ID = 10;
const root = path.join(__dirname, '..');

function makeUser(overrides = {}) {
  return {
    id: 19,
    name: 'Jake',
    email: 'jzmaynard7@gmail.com',
    client_id: CLIENT_ID,
    role: 'ao',
    active: true,
    ...overrides,
  };
}

function matchesNameFallback(name) {
  return /Jacob Maynard/i.test(name)
    || /Jake Maynard/i.test(name)
    || String(name).trim().toLowerCase() === 'jake';
}

function clearAppModules() {
  for (const key of Object.keys(require.cache)) {
    if (!key.startsWith(root)) continue;
    if (key.includes(`${path.sep}node_modules${path.sep}`)) continue;
    if (key.startsWith(path.join(root, 'test'))) continue;
    delete require.cache[key];
  }
}

function installUserMock(pool, users) {
  const originalQuery = pool.query;

  pool.query = async (sql, params = []) => {
    const normalized = sql.replace(/\s+/g, ' ').trim();

    if (/lower\(email\) = lower/.test(normalized)) {
      const [email, clientId, roles] = params;
      const match = users.find(u =>
        u.email.toLowerCase() === String(email).toLowerCase()
        && u.active === true
        && u.client_id === clientId
        && roles.includes(u.role));
      return { rows: match ? [match] : [] };
    }

    if (/trim\(name\) ILIKE 'Jake'/.test(normalized)) {
      const [clientId, roles] = params;
      const matches = users.filter(u =>
        u.active === true
        && u.client_id === clientId
        && roles.includes(u.role)
        && matchesNameFallback(u.name));
      matches.sort((a, b) => {
        if (a.role === 'ao' && b.role !== 'ao') return -1;
        if (b.role === 'ao' && a.role !== 'ao') return 1;
        return a.id - b.id;
      });
      return { rows: matches.slice(0, 1) };
    }

    return originalQuery(sql, params);
  };

  return () => {
    pool.query = originalQuery;
  };
}

function loadAoFieldService(users) {
  clearAppModules();
  const pool = require('../db');
  const restore = installUserMock(pool, users);
  const aoField = require('../services/aoFieldService');
  return { aoField, restore };
}

test('resolveJakeAoOwner resolves active Jake on tenant 10 via production email', async () => {
  const originalJakeEmail = process.env.JAKE_EMAIL;
  delete process.env.JAKE_EMAIL;
  const { aoField, restore } = loadAoFieldService([makeUser()]);

  try {
    const jake = await aoField.resolveJakeAoOwner(CLIENT_ID);
    assert.ok(jake);
    assert.equal(jake.id, 19);
    assert.equal(jake.email, 'jzmaynard7@gmail.com');
    assert.equal(jake.role, 'ao');
    assert.equal(jake.client_id, CLIENT_ID);
    assert.equal(jake.active, true);
  } finally {
    restore();
    clearAppModules();
    if (originalJakeEmail === undefined) delete process.env.JAKE_EMAIL;
    else process.env.JAKE_EMAIL = originalJakeEmail;
  }
});

test('resolveJakeAoOwner ignores inactive duplicate on production email', async () => {
  const originalJakeEmail = process.env.JAKE_EMAIL;
  delete process.env.JAKE_EMAIL;
  const { aoField, restore } = loadAoFieldService([
    makeUser({ id: 19, active: false }),
    makeUser({
      id: 99,
      email: 'jacob@gopulseforge.com',
      role: 'admin',
      name: 'Jacob Maynard',
    }),
  ]);

  try {
    const jake = await aoField.resolveJakeAoOwner(CLIENT_ID);
    assert.ok(jake);
    assert.equal(jake.email, 'jacob@gopulseforge.com');
    assert.notEqual(jake.id, 19);
  } finally {
    restore();
    clearAppModules();
    if (originalJakeEmail === undefined) delete process.env.JAKE_EMAIL;
    else process.env.JAKE_EMAIL = originalJakeEmail;
  }
});

test('resolveJakeAoOwner does not resolve Jake on wrong tenant', async () => {
  const originalJakeEmail = process.env.JAKE_EMAIL;
  delete process.env.JAKE_EMAIL;
  const { aoField, restore } = loadAoFieldService([
    makeUser({ client_id: 1 }),
  ]);

  try {
    const jake = await aoField.resolveJakeAoOwner(CLIENT_ID);
    assert.equal(jake, null);
  } finally {
    restore();
    clearAppModules();
    if (originalJakeEmail === undefined) delete process.env.JAKE_EMAIL;
    else process.env.JAKE_EMAIL = originalJakeEmail;
  }
});

test('resolveJakeAoOwner prefers JAKE_EMAIL override when active on tenant 10', async () => {
  const originalJakeEmail = process.env.JAKE_EMAIL;
  process.env.JAKE_EMAIL = 'override@example.com';
  const { aoField, restore } = loadAoFieldService([
    makeUser(),
    makeUser({
      id: 77,
      email: 'override@example.com',
      name: 'Jake Override',
    }),
  ]);

  try {
    const jake = await aoField.resolveJakeAoOwner(CLIENT_ID);
    assert.ok(jake);
    assert.equal(jake.id, 77);
    assert.equal(jake.email, 'override@example.com');
  } finally {
    restore();
    clearAppModules();
    if (originalJakeEmail === undefined) delete process.env.JAKE_EMAIL;
    else process.env.JAKE_EMAIL = originalJakeEmail;
  }
});

test('resolveJakeAoOwner falls back to tenant-10 name match when emails miss', async () => {
  const originalJakeEmail = process.env.JAKE_EMAIL;
  delete process.env.JAKE_EMAIL;
  const { aoField, restore } = loadAoFieldService([
    makeUser({
      id: 55,
      email: 'other@example.com',
      name: 'Jake',
      role: 'ao',
    }),
  ]);

  try {
    const jake = await aoField.resolveJakeAoOwner(CLIENT_ID);
    assert.ok(jake);
    assert.equal(jake.id, 55);
    assert.equal(jake.name, 'Jake');
  } finally {
    restore();
    clearAppModules();
    if (originalJakeEmail === undefined) delete process.env.JAKE_EMAIL;
    else process.env.JAKE_EMAIL = originalJakeEmail;
  }
});
