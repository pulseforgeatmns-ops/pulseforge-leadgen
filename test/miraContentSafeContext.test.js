const assert = require('node:assert/strict');
const test = require('node:test');
const { buildMiraContext } = require('../utils/miraContext');

function contentSafeQuery(sql, params) {
  assert.deepEqual(params, [10]);
  if (/FROM clients\s+WHERE id = \$1/i.test(sql)) {
    return Promise.resolve({ rows: [{ id: 10, name: 'Anchor Cleaning', city: 'Manchester', state: 'NH' }] });
  }
  if (/AS send_count_24h/i.test(sql)) {
    assert.match(sql, /client_id = \$1/g);
    return Promise.resolve({ rows: [{
      send_count_24h: 10,
      open_count_24h: 7,
      reply_count_24h: 2,
      bounce_count_24h: 0,
      warm_signal_count_24h: 3,
      send_daily_average_previous_7d: 8,
    }] });
  }
  if (/GROUP BY \(ran_at AT TIME ZONE/i.test(sql)) {
    assert.match(sql, /client_id = \$1/);
    return Promise.resolve({ rows: [{ activity_date: '2026-07-05', send_count: 10 }] });
  }
  if (/FROM linkedin_post_stats/i.test(sql)) {
    return Promise.resolve({ rows: [{
      posted_at: '2026-07-07T14:00:00.000Z',
      format: 'text',
      hook_type: 'claim',
      impressions: 1200,
      members_reached: 900,
      engagement_rate: '4.25',
      first_hour_active: true,
    }] });
  }
  throw new Error(`Unexpected content-safe query: ${sql.slice(0, 80)}`);
}

test('content-safe Mira context is client-scoped and omits sensitive collections', async () => {
  const query = async (sql, params = []) => {
    if (/to_regclass\('public\.daily_anchors'\)/i.test(sql)) return { rows: [{ tbl: null }] };
    return contentSafeQuery(sql, params);
  };

  const context = await buildMiraContext(10, {
    contentSafe: true,
    channel: 'linkedin_page',
    query,
  });

  assert.equal(context.available, true);
  assert.deepEqual(context.client, { id: 10, name: 'Anchor Cleaning', city: 'Manchester', state: 'NH' });
  assert.deepEqual(context.metrics, { sends_24h: 10, opens_24h: 7, replies_24h: 2, warm_signals_24h: 3 });
  assert.deepEqual(context.linkedin_post_stats, [{
    posted_at: '2026-07-07T14:00:00.000Z',
    format: 'text',
    hook_type: 'claim',
    impressions: 1200,
    members_reached: 900,
    engagement_rate: 4.25,
    first_hour_active: true,
  }]);
  assert.match(context.recent_activity_summaries.join('\n'), /Manchester/);

  const serialized = JSON.stringify(context);
  for (const forbidden of [
    'recent_captures', 'active_tasks', 'stale_tasks', 'open_blockers',
    'recent_client_notes', 'recent_corrections', 'linked_entity_id',
    'email', 'phone', 'linkedin_url', 'task_id', 'blocker_id',
  ]) {
    assert.doesNotMatch(serialized, new RegExp(`"${forbidden}"\\s*:`, 'i'));
  }
  assert.doesNotMatch(serialized, /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  assert.doesNotMatch(serialized, /linkedin\.com\//i);
});

test('content-safe Mira context reports unavailable instead of inventing metrics', async () => {
  const context = await buildMiraContext(1, {
    contentSafe: true,
    channel: 'facebook_page',
    query: async () => { throw new Error('Mira unavailable'); },
  });

  assert.equal(context.available, false);
  assert.deepEqual(context.metrics, { sends_24h: 0, opens_24h: 0, replies_24h: 0, warm_signals_24h: 0 });
  assert.deepEqual(context.recent_activity_summaries, []);
});

test('content-safe Mira context reads exact client-scoped anchor text', async () => {
  const anchorText = 'Warm signals climbing four days straight in Providence, 10 to 41 in the last 24h. Opens up, replies still at 0. Engagement leads response, and the gap is where follow-up lives.';
  const query = async (sql, params = []) => {
    if (/to_regclass\('public\.daily_anchors'\)/i.test(sql)) return { rows: [{ tbl: 'daily_anchors' }] };
    if (/information_schema\.columns/i.test(sql)) {
      return { rows: ['id', 'client_id', 'anchor_date', 'primary_anchor'].map(column_name => ({ column_name })) };
    }
    if (/FROM daily_anchors/i.test(sql)) {
      assert.deepEqual(params, [1]);
      return { rows: [{ client_id: 1, anchor_date: '2026-07-10', primary_anchor: anchorText }] };
    }
    if (/FROM clients\s+WHERE id = \$1/i.test(sql)) {
      return { rows: [{ id: 1, name: 'Pulseforge', city: 'Providence', state: 'RI' }] };
    }
    if (/AS send_count_24h/i.test(sql)) {
      return { rows: [{
        send_count_24h: 0,
        open_count_24h: 0,
        reply_count_24h: 0,
        bounce_count_24h: 0,
        warm_signal_count_24h: 0,
        send_daily_average_previous_7d: 0,
      }] };
    }
    if (/GROUP BY \(ran_at AT TIME ZONE/i.test(sql)) return { rows: [] };
    if (/FROM linkedin_post_stats/i.test(sql)) return { rows: [] };
    throw new Error(`Unexpected anchor query: ${sql.slice(0, 80)}`);
  };

  const context = await buildMiraContext(1, {
    contentSafe: true,
    channel: 'linkedin_page',
    query,
  });

  assert.equal(context.current_anchor.primary_anchor, anchorText);
});

test('full Mira builder preserves the HTTP endpoint response shape', async () => {
  const context = await buildMiraContext(null, {
    contentSafe: false,
    includeCrossClient: false,
    query: async () => ({ rows: [] }),
  });
  assert.deepEqual(Object.keys(context).sort(), [
    'active_clients',
    'active_tasks',
    'current_anchor',
    'daily_health_today',
    'daily_health_trend_7d',
    'daily_health_yesterday',
    'live_workstreams',
    'now',
    'open_blockers',
    'open_tasks_count',
    'recent_captures',
    'recent_client_notes',
    'recent_corrections',
    'stale_tasks',
  ]);
});
