const assert = require('node:assert/strict');
const pool = require('../db');
const { _test } = require('../leadgen');

function makePool(rows) {
  const queue = rows.map(row => ({ ...row }));
  const calls = [];
  return {
    queue,
    calls,
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (/SELECT id, client_id, industry, vertical, location, prospect_count, parked_at/i.test(sql)) {
        const clientId = Number(params[0]);
        const allowed = Array.isArray(params[1]) ? params[1] : null;
        return {
          rows: queue
            .filter(row => Number(row.client_id) === clientId)
            .filter(row => row.status === 'queued' && row.saturated === false)
            .filter(row => !allowed || allowed.includes(row.vertical))
            .sort((a, b) => (a.prospect_count - b.prospect_count) || (a.id - b.id)),
        };
      }
      if (/SELECT id, client_id, vertical, location, parked_at/i.test(sql)) {
        const clientId = Number(params[0]);
        return {
          rows: queue
            .filter(row => Number(row.client_id) === clientId)
            .sort((a, b) => a.vertical.localeCompare(b.vertical) || a.location.localeCompare(b.location) || (a.id - b.id)),
        };
      }
      if (/UPDATE scout_queue\s+SET parked_at = NOW\(\)/i.test(sql)) {
        const row = queue.find(item => item.id === params[0]);
        if (row) row.parked_at = new Date('2026-07-09T18:00:00.000Z');
        return { rows: [] };
      }
      if (/UPDATE scout_queue\s+SET parked_at = NULL/i.test(sql)) {
        const row = queue.find(item => item.id === params[0]);
        if (row) row.parked_at = null;
        return { rows: [] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    },
  };
}

async function withMockPool(rows, fn) {
  const originalQuery = pool.query;
  const mock = makePool(rows);
  pool.query = mock.query.bind(mock);
  try {
    return await fn(mock);
  } finally {
    pool.query = originalQuery;
  }
}

async function withCapturedWarn(fn) {
  const originalWarn = console.warn;
  const warnings = [];
  console.warn = (...args) => warnings.push(args.join(' '));
  try {
    await fn(warnings);
  } finally {
    console.warn = originalWarn;
  }
}

async function run() {
  await withMockPool([
    { id: 1, client_id: 10, industry: 'law_firm', vertical: 'law_firm', location: 'Londonderry WV', prospect_count: 0, saturated: false, status: 'queued', parked_at: null },
    { id: 2, client_id: 10, industry: 'law_firm', vertical: 'law_firm', location: 'Auburn NH', prospect_count: 0, saturated: false, status: 'queued', parked_at: null },
  ], async mock => {
    const next = await _test.pickNextQueueItem(10, ['law_firm']);
    assert.equal(next.location, 'Auburn NH');
    assert.equal(mock.queue.find(row => row.id === 1).parked_at instanceof Date, true);
  });

  await withMockPool([
    { id: 10, client_id: 10, industry: 'law_firm', vertical: 'law_firm', location: 'Manchester WV', prospect_count: 0, saturated: false, status: 'queued', parked_at: null },
    { id: 11, client_id: 10, industry: 'law_firm', vertical: 'law_firm', location: 'Manchester NH', prospect_count: 1, saturated: false, status: 'queued', parked_at: null },
    { id: 20, client_id: 2, industry: 'property_management', vertical: 'property_management', location: 'Charleston WV', prospect_count: 0, saturated: false, status: 'queued', parked_at: null },
  ], async mock => {
    const stats = await _test.reconcileScoutQueueForClient(10);
    assert.equal(stats.parked, 1);
    assert.equal(stats.unparked, 0);
    assert.equal(mock.queue.find(row => row.id === 10).parked_at instanceof Date, true);
    assert.equal(mock.queue.find(row => row.id === 11).parked_at, null);
    assert.equal(mock.queue.find(row => row.id === 20).parked_at, null);
  });

  const originalState = _test.CLIENT_SCOUT_PLANS[10].state;
  try {
    _test.CLIENT_SCOUT_PLANS[10].state = 'WV';
    await withMockPool([
      { id: 30, client_id: 10, industry: 'law_firm', vertical: 'law_firm', location: 'Manchester WV', prospect_count: 0, saturated: false, status: 'queued', parked_at: new Date('2026-07-09T17:00:00.000Z') },
    ], async mock => {
      const stats = await _test.reconcileScoutQueueForClient(10);
      assert.equal(stats.parked, 0);
      assert.equal(stats.unparked, 1);
      assert.equal(mock.queue.find(row => row.id === 30).parked_at, null);
    });
  } finally {
    _test.CLIENT_SCOUT_PLANS[10].state = originalState;
  }

  await withCapturedWarn(async warnings => {
    await withMockPool([
      { id: 40, client_id: 10, industry: 'law_firm', vertical: 'law_firm', location: 'Manchester WV', prospect_count: 0, saturated: false, status: 'queued', parked_at: null },
      { id: 41, client_id: 10, industry: 'law_firm', vertical: 'law_firm', location: 'Bedford WV', prospect_count: 0, saturated: false, status: 'queued', parked_at: null },
    ], async () => {
      const next = await _test.pickNextQueueItem(10, ['law_firm']);
      assert.equal(next, null);
    });
    assert.match(warnings.join('\n'), /No valid queue items for client 10/);
    assert.match(warnings.join('\n'), /skipped 2 invalid rows/);
  });

  console.log('Scout queue parking tests passed');
}

run().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
