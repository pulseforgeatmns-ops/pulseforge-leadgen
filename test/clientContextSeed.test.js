const assert = require('node:assert/strict');
const test = require('node:test');

const dbPath = require.resolve('../db');
const contextPath = require.resolve('../utils/clientContext');
const preserved = {
  id: 1,
  service_area: ['Custom market'],
  verticals: ['custom_vertical'],
  target_clients: 'custom ICP',
  sender_email: 'owner@custom.example',
  sender_name: 'Custom Owner',
  sending_domain: 'custom.example',
  enabled_agents: ['scout'],
  active: false,
};
const queries = [];

require.cache[dbPath] = {
  id: dbPath,
  filename: dbPath,
  loaded: true,
  exports: {
    async query(sql) {
      queries.push(sql);
      // A tiny simulated database: existing ids make seed inserts no-ops. The
      // mutation branches model precisely the regressions this test prevents.
      if (/INSERT INTO clients/i.test(sql)) {
        if (/ON CONFLICT \(id\) DO UPDATE/i.test(sql)) {
          preserved.service_area = ['seed market'];
          preserved.verticals = ['seed_vertical'];
          preserved.target_clients = 'seed ICP';
          preserved.sender_email = 'seed@example.test';
          preserved.active = true;
        }
        return { rows: [] };
      }
      if (/UPDATE clients\s+SET active = true WHERE active IS NULL/i.test(sql)) return { rows: [] };
      if (/UPDATE clients\s+SET enabled_agents = CASE/i.test(sql)) return { rows: [] };
      if (/UPDATE clients\s+SET enabled_agents = ARRAY/i.test(sql)) {
        preserved.enabled_agents = ['seed_agent'];
        return { rows: [] };
      }
      if (/UPDATE clients SET\s+sender_email/i.test(sql)) {
        preserved.sender_email = 'seed@example.test';
        return { rows: [] };
      }
      return { rows: [] };
    },
  },
};
delete require.cache[contextPath];
const { ensureClientArchitecture } = require('../utils/clientContext');

test('simulated boot preserves an existing client configuration', async () => {
  await ensureClientArchitecture();

  assert.deepEqual(preserved.service_area, ['Custom market']);
  assert.deepEqual(preserved.verticals, ['custom_vertical']);
  assert.equal(preserved.target_clients, 'custom ICP');
  assert.equal(preserved.sender_email, 'owner@custom.example');
  assert.equal(preserved.sender_name, 'Custom Owner');
  assert.equal(preserved.sending_domain, 'custom.example');
  assert.deepEqual(preserved.enabled_agents, ['scout']);
  assert.equal(preserved.active, false);
  assert.equal(queries.filter(sql => /INSERT INTO clients/i.test(sql) && /ON CONFLICT \(id\) DO NOTHING/i.test(sql)).length, 3);
  assert.equal(queries.some(sql => /sender_email = 'jacob@gopulseforge\.com'/i.test(sql)), false);
});
