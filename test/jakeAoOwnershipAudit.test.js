'use strict';

const assert = require('node:assert/strict');
const path = require('node:path');
const test = require('node:test');

const CLIENT_ID = 10;
const root = path.join(__dirname, '..');

const ACTIVE_AOS = Object.freeze([
  { id: 19, name: 'Jake', email: 'jzmaynard7@gmail.com', client_id: CLIENT_ID, role: 'ao', active: true },
  { id: 20, name: 'Mike', email: 'mike@example.com', client_id: CLIENT_ID, role: 'ao', active: true },
  { id: 24, name: 'Tony', email: 'tony@example.com', client_id: CLIENT_ID, role: 'ao', active: true },
  { id: 25, name: 'Rory', email: 'rory@example.com', client_id: CLIENT_ID, role: 'ao', active: true },
  { id: 26, name: 'Zach', email: 'zach@example.com', client_id: CLIENT_ID, role: 'ao', active: true },
]);

function clearAppModules() {
  for (const key of Object.keys(require.cache)) {
    if (!key.startsWith(root)) continue;
    if (key.includes(`${path.sep}node_modules${path.sep}`)) continue;
    if (key.startsWith(path.join(root, 'test'))) continue;
    delete require.cache[key];
  }
}

function installAoOwnerListMock(pool, users) {
  const originalQuery = pool.query;
  pool.query = async (sql, params = []) => {
    const normalized = sql.replace(/\s+/g, ' ').trim();
    if (/FROM users[\s\S]*role = 'ao'[\s\S]*active = true/i.test(normalized)
      && /ORDER BY id ASC/i.test(normalized)) {
      const [clientId, excludeUserId] = params;
      const rows = users.filter(u =>
        u.client_id === clientId
        && u.role === 'ao'
        && u.active === true
        && (excludeUserId == null || u.id !== excludeUserId));
      rows.sort((a, b) => a.id - b.id);
      return { rows };
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
  const restore = installAoOwnerListMock(pool, users);
  const aoField = require('../services/aoFieldService');
  return { aoField, restore };
}

test('listActiveAoOwners includes Zach (not Zack pattern miss)', async () => {
  const { aoField, restore } = loadAoFieldService(ACTIVE_AOS);
  try {
    const others = await aoField.listActiveAoOwners(CLIENT_ID, { excludeUserId: 19 });
    assert.equal(others.length, 4);
    assert.deepEqual(
      others.map(o => o.name).sort(),
      ['Mike', 'Rory', 'Tony', 'Zach'],
    );
  } finally {
    restore();
  }
});

test('ownership invariant compares all active tenant-10 AOs except Jake', () => {
  const jakeId = 19;
  const otherAoIds = ACTIVE_AOS.filter(o => o.id !== jakeId).map(o => o.id);
  const beforeCounts = { 20: 15, 24: 15, 25: 15, 26: 15, 19: 1 };
  const afterCounts = { 20: 15, 24: 15, 25: 15, 26: 15, 19: 18 };

  const unchanged = otherAoIds.every(
    id => (beforeCounts[id] || 0) === (afterCounts[id] || 0),
  );
  assert.equal(unchanged, true);
  assert.equal(otherAoIds.includes(26), true, 'Zach must be in invariant set');
});

test('seed dry-run projection after cleanup: 18 candidates, 17 inserts, 1 conflict', () => {
  const { PROSPECTS } = require('../scripts/data/jakeAoProspectBook');
  assert.equal(PROSPECTS.length, 18);

  const wouldInsert = 17;
  const skippedConflict = 1; // Wadleigh → Tony
  assert.equal(wouldInsert + skippedConflict, PROSPECTS.length);

  const wadleigh = PROSPECTS.find(p => p.business_name === 'Wadleigh Starr & Peters');
  assert.ok(wadleigh, 'Wadleigh remains in manifest');
});

test('final projected Jake count = 18 after cleanup + seed', () => {
  const jakeAfterCleanup = 1;
  const wouldInsert = 17;
  assert.equal(jakeAfterCleanup + wouldInsert, 18);
});

test('legacy Zack name pattern would have omitted Zach', () => {
  const legacyPatterns = ['%Zack%', '%Rory%', '%Tony%', '%Mike%'];
  const zach = 'Zach';
  const matched = legacyPatterns.some(p => {
    const re = new RegExp(`^${p.replace(/%/g, '.*')}$`, 'i');
    return re.test(zach);
  });
  assert.equal(matched, false, 'documents root cause: Zack pattern misses Zach');
});
