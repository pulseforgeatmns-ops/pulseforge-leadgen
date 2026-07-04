const assert = require('node:assert/strict');
const test = require('node:test');

const dbPath = require.resolve('../db');
const agentPath = require.resolve('../warmRoutingAgent');
const queries = [];

require.cache[dbPath] = {
  id: dbPath,
  filename: dbPath,
  loaded: true,
  exports: {
    async query(sql, params = []) {
      queries.push({ sql, params });
      if (/INSERT INTO warm_signal_events/.test(sql)) return { rows: [{ id: queries.length }] };
      if (/SELECT COUNT\(\*\)::int AS count/.test(sql)) return { rows: [{ count: 0 }] };
      if (/INSERT INTO warm_trigger_fires/.test(sql)) {
        return { rows: [{ id: 777, fired_at: new Date('2026-07-04T12:00:00Z') }] };
      }
      if (/INSERT INTO capture_inbox/.test(sql)) return { rows: [{ id: 888 }] };
      return { rows: [] };
    },
  },
};

delete require.cache[agentPath];
const {
  buildCurrentEdgeEvents,
  classifyIcpScoreChange,
  groupSignalEventsByProspect,
  isWarmRoutingEnabled,
  processProspectEvents,
  triggerLabel,
} = require('../warmRoutingAgent');

test.beforeEach(() => {
  queries.length = 0;
  process.env.MIRA_TELEGRAM_BOT_TOKEN = '';
  process.env.JACOB_TELEGRAM_CHAT_ID = '';
});

test('a prospect satisfying multiple labels plans one incident fire', () => {
  const prospect = {
    id: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    client_id: 1,
    icp_score: 92,
    email_touched_at: new Date(Date.now() - 60_000).toISOString(),
    opens_24h: 3,
    clicks_24h: 0,
    engagement_event_key: 'email_event:5000',
  };

  const { events } = buildCurrentEdgeEvents(prospect, new Map());
  assert.deepEqual(events.map(event => event.reason), [
    'ICP_CROSS_80_RECENT',
    'ENGAGEMENT_CLUSTER',
  ]);

  const incidents = groupSignalEventsByProspect(events);
  assert.equal(incidents.size, 1);
  assert.equal(incidents.get(prospect.id).length, 2);
});

test('multiple claimed labels create one warm_trigger_fires row', async () => {
  const prospect = {
    id: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    client_id: 1,
    email: 'owner@example.com',
    company_name: 'Example Company',
    icp_score: 92,
    opens_24h: 3,
    clicks_24h: 0,
  };
  const events = [
    {
      prospect_id: prospect.id,
      signal_type: 'ICP_ELIGIBILITY',
      reason: 'ICP_CROSS_80_RECENT',
      event_key: 'icp_eligibility:example',
      observed_at: '2026-07-04T11:00:00Z',
      trigger_label: 'ICP crossed 80, now 92',
      evidence: { icp_score: 92 },
    },
    {
      prospect_id: prospect.id,
      signal_type: 'ENGAGEMENT_CLUSTER',
      reason: 'ENGAGEMENT_CLUSTER',
      event_key: 'engagement_edge:example',
      observed_at: '2026-07-04T11:01:00Z',
      trigger_label: '3 opens in 24h',
      evidence: { opens_24h: 3, clicks_24h: 0 },
    },
  ];

  const result = await processProspectEvents(prospect, events);
  assert.equal(result.fired, true);
  assert.equal(result.consumed, 2);
  assert.equal(queries.filter(({ sql }) => /INSERT INTO warm_trigger_fires/.test(sql)).length, 1);
});

test('seeded active thresholds project zero first-run fires', () => {
  const prospect = {
    id: '1000c166-c9c3-4bab-adef-d4cbdf14ab18',
    client_id: 1,
    icp_score: 92,
    email_touched_at: new Date(Date.now() - 60_000).toISOString(),
    opens_24h: 3,
    clicks_24h: 0,
    engagement_event_key: 'email_event:5000',
  };
  const states = new Map([
    [`${prospect.id}:ICP_ELIGIBILITY`, { is_active: true }],
    [`${prospect.id}:ENGAGEMENT_CLUSTER`, { is_active: true }],
  ]);

  assert.equal(buildCurrentEdgeEvents(prospect, states).events.length, 0);
});

test('80 to 92 is a one-shot higher-tier crossing', () => {
  const event = classifyIcpScoreChange({
    id: 99,
    old_score: 80,
    new_score: 92,
    reason: 'riley:engagement',
    created_at: '2026-07-04T12:00:00Z',
  });

  assert.equal(event.reason, 'ICP_CROSS_90');
  assert.equal(event.event_key, 'icp_history:99');
  assert.equal(event.evidence.crossed_90, true);
  assert.equal(event.evidence.jumped_15, false);
});

test('auto escalation renders the stored engagement label', () => {
  assert.equal(triggerLabel('ENGAGEMENT_CLUSTER', {
    trigger_label: '4 opens in 24h',
  }), '4 opens in 24h');
});

test('warm routing is default-off and requires an explicit true value', () => {
  assert.equal(isWarmRoutingEnabled({}), false);
  assert.equal(isWarmRoutingEnabled({ WARM_ROUTING_ENABLED: 'false' }), false);
  assert.equal(isWarmRoutingEnabled({ WARM_ROUTING_ENABLED: 'true' }), true);
});
