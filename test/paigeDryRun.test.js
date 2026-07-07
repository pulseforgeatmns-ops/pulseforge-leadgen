const assert = require('node:assert/strict');
const test = require('node:test');

process.env.ACTIVE_CLIENT_ID = '10';

const writes = [];
const dbPath = require.resolve('../db');
require.cache[dbPath] = {
  id: dbPath,
  filename: dbPath,
  loaded: true,
  exports: {
    query: async (sql, params = []) => {
      const normalized = String(sql).trim();
      if (/^(?:INSERT|UPDATE|ALTER|DELETE|CREATE|DROP|DO)\b/i.test(normalized)) {
        writes.push(normalized);
        throw new Error(`Dry run attempted a write: ${normalized.slice(0, 40)}`);
      }
      if (/SELECT \* FROM clients WHERE id = \$1 AND active = true/i.test(sql)) {
        return { rows: [{
          id: 10,
          name: 'Anchor Cleaning',
          business_name: 'Anchor Cleaning',
          vertical: 'commercial_cleaning',
          city: 'Manchester',
          state: 'NH',
          enabled_agents: ['scout'],
        }] };
      }
      if (/FROM clients\s+WHERE id = \$1 AND active = true/i.test(sql)) {
        return { rows: [{ id: 10, name: 'Anchor Cleaning', city: 'Manchester', state: 'NH' }] };
      }
      if (/AS send_count_24h/i.test(sql)) {
        return { rows: [{
          send_count_24h: 10,
          open_count_24h: 7,
          reply_count_24h: 2,
          bounce_count_24h: 0,
          warm_signal_count_24h: 3,
          send_daily_average_previous_7d: 8,
        }] };
      }
      if (/GROUP BY \(ran_at AT TIME ZONE/i.test(sql)) {
        return { rows: [{ activity_date: '2026-07-05', send_count: 10 }] };
      }
      if (/to_regclass\('public\.daily_anchors'\)/i.test(sql)) return { rows: [{ tbl: null }] };
      return { rows: [] };
    },
  },
};

const anthropicPath = require.resolve('@anthropic-ai/sdk');
class FakeAnthropic {
  constructor() {
    this.messages = {
      create: async request => {
        const prompt = request.messages?.[0]?.content || '';
        if (/Score this social media post/i.test(prompt)) {
          return { content: [{ text: JSON.stringify({
            specificity: 9,
            originality: 9,
            hook_strength: 9,
            total: 27,
            weak_dimension: 'none',
            reason: 'Specific, grounded, and direct.',
          }) }] };
        }
        return { content: [{ text: JSON.stringify({
          format: 'dialogue',
          post_body: 'Practice Manager: "Ten sends means the pipeline is fixed."\n\nMe: [pause] "Two replies means we have a signal, not a guarantee."\n\nPractice Manager: "So what changes?"\n\nMe: "We keep the scope narrow and own the NEXT step."',
          hashtags: [],
          source_anchors: ['Mira: 10 sends over the past 24 hours', 'Mira: 2 replies over the past 24 hours'],
        }) }] };
      },
    };
  }
}
require.cache[anthropicPath] = {
  id: anthropicPath,
  filename: anthropicPath,
  loaded: true,
  exports: FakeAnthropic,
};

const { run } = require('../paigeAgent');

test('Paige dry-run generates Anchor content without any database write', async () => {
  const result = await run({ client_id: 10, dryRun: true, channel: 'linkedin_page', format: 'dialogue' });
  assert.equal(result.success, true);
  assert.equal(result.dry_run, true);
  assert.equal(result.outputs.length, 1);
  assert.equal(result.outputs[0].meta.brand, 'anchor');
  assert.equal(result.outputs[0].meta.format, 'dialogue');
  assert.match(result.outputs[0].content, /Practice Manager:/);
  assert.doesNotMatch(result.outputs[0].content, /Pulseforge/i);
  assert.deepEqual(writes, []);
});

test('Anchor production generation remains blocked while enabled_agents is Scout-only', async () => {
  const result = await run({ client_id: 10, dryRun: false, channel: 'linkedin_page' });
  assert.equal(result.skipped, true);
  assert.equal(result.reason, 'anchor_dry_run_only');
  assert.deepEqual(writes, []);
});

test('dry-run sequence rotates through three different LinkedIn formats without writes', async () => {
  const result = await run({ client_id: 10, dryRun: true, channel: 'linkedin_page', count: 3 });
  assert.equal(result.success, true);
  assert.equal(result.outputs.length, 3);
  assert.equal(new Set(result.outputs.map(output => output.meta.format)).size, 3);
  assert.deepEqual(writes, []);
});

test('Mira-unavailable dry-run aborts without generating or writing', async () => {
  const result = await run({
    client_id: 10,
    dryRun: true,
    channel: 'facebook_page',
    simulateMiraUnavailable: true,
  });
  assert.equal(result.success, false);
  assert.equal(result.outputs.length, 0);
  assert.match(result.channels_failed[0], /facebook_page/);
  assert.deepEqual(writes, []);
});
