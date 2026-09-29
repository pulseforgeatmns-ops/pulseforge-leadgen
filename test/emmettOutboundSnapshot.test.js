'use strict';

/**
 * Durable Emmett inbox age — independent of 7-day reputation windows.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildInboxSnapshot,
  resolveInboxAge,
  FIRST_SEND_EVENT_TYPES,
} = require('../services/emmettOutboundSnapshot');

const NOW = new Date('2026-08-29T17:00:00.000Z');

function daysAgo(n) {
  return new Date(NOW.getTime() - n * 86400000);
}

function createMockPool(handlers) {
  const queries = [];
  return {
    queries,
    async query(sql, params = []) {
      queries.push({ sql: String(sql), params });
      for (const handler of handlers) {
        const result = handler(sql, params);
        if (result) return result;
      }
      return { rows: [] };
    },
  };
}

function defaultHandlers(overrides = {}) {
  const state = {
    firstSendAt: 'firstSendAt' in overrides ? overrides.firstSendAt : daysAgo(47),
    recentSends: overrides.recentSends ?? 80,
    bounces: overrides.bounces ?? 0,
    opens: overrides.opens ?? 50,
    replies: overrides.replies ?? 8,
    complaints: overrides.complaints ?? 0,
    warmupStartDate: overrides.warmupStartDate ?? null,
    createdAt: overrides.createdAt ?? daysAgo(120),
    sentToday: overrides.sentToday ?? 0,
    sentYesterday: overrides.sentYesterday ?? 18,
    historicalDailyAvg: overrides.historicalDailyAvg ?? 16,
  };

  return [
    (sql) => {
      if (/FROM clients WHERE id/.test(sql)) {
        return {
          rows: [{
            id: 21,
            sender_email: 'outbound@example.com',
            sending_domain: 'example.com',
            created_at: state.createdAt,
            warmup_start_date: state.warmupStartDate,
            autosend_enabled: true,
          }],
        };
      }
      return null;
    },
    (sql, params) => {
      if (/INTERVAL '7 days'/.test(sql) && /FROM email_events/.test(sql)) {
        return {
          rows: [{
            sends: state.recentSends,
            bounces: state.bounces,
            opens: state.opens,
            replies: state.replies,
            complaints: state.complaints,
          }],
        };
      }
      return null;
    },
    (sql, params) => {
      if (/MIN\(event_at\)/.test(sql) && /FROM email_events/.test(sql)) {
        assert.doesNotMatch(sql, /INTERVAL '7 days'/, 'first-send lookup must not use reputation window');
        assert.deepEqual(params, [21, FIRST_SEND_EVENT_TYPES]);
        return { rows: [{ first_sent_at: state.firstSendAt }] };
      }
      return null;
    },
    (sql) => {
      if (/sent_today/.test(sql)) return { rows: [{ sent_today: state.sentToday }] };
      return null;
    },
    (sql) => {
      if (/sent_yesterday/.test(sql)) return { rows: [{ sent_yesterday: state.sentYesterday }] };
      return null;
    },
    (sql) => {
      if (/historical_daily_avg/.test(sql)) {
        return { rows: [{ historical_daily_avg: state.historicalDailyAvg }] };
      }
      return null;
    },
  ];
}

describe('resolveInboxAge precedence', () => {
  it('prefers durable first_send over warmup and created_at', () => {
    const result = resolveInboxAge({
      firstSentAt: daysAgo(47),
      warmupStartDate: '2026-07-01',
      createdAt: daysAgo(120),
      now: NOW,
    });
    assert.equal(result.inboxAgeSource, 'first_send');
    assert.equal(result.inboxAgeDays, 47);
  });

  it('falls back to warmup_start_date when no send history exists', () => {
    const warmupStartDate = daysAgo(45);
    const result = resolveInboxAge({
      firstSentAt: null,
      warmupStartDate,
      createdAt: daysAgo(120),
      now: NOW,
    });
    assert.equal(result.inboxAgeSource, 'warmup_start_date');
    assert.equal(result.inboxAgeDays, 45);
  });

  it('falls back to created_at when no send history or warmup date exists', () => {
    const result = resolveInboxAge({
      firstSentAt: null,
      warmupStartDate: null,
      createdAt: daysAgo(90),
      now: NOW,
    });
    assert.equal(result.inboxAgeSource, 'created_at');
    assert.equal(result.inboxAgeDays, 90);
  });

  it('uses safe default without fabricating a first-send timestamp', () => {
    const result = resolveInboxAge({
      firstSentAt: null,
      warmupStartDate: null,
      createdAt: null,
      now: NOW,
      fallbackAgeDays: 0,
    });
    assert.equal(result.inboxAgeSource, 'default');
    assert.equal(result.inboxAgeDays, 0);
    assert.equal(result.inboxAgeAnchor, null);
  });
});

describe('buildInboxSnapshot durable inbox age', () => {
  it('keeps a mature active inbox mature when recent telemetry spans only 7 days', async () => {
    const pool = createMockPool(defaultHandlers({
      firstSendAt: daysAgo(47),
      recentSends: 80,
    }));

    const snapshot = await buildInboxSnapshot(21, { pool, now: NOW, warmupStages: [] });

    assert.equal(snapshot.inboxAgeDays, 47);
    assert.equal(snapshot.inboxAgeSource, 'first_send');
    assert.equal(snapshot.recentSends, 80);
    assert.notEqual(snapshot.inboxAgeDays, 6);
    assert.notEqual(snapshot.inboxAgeDays, 7);
  });

  it('preserves age when no sends exist in the recent reputation window', async () => {
    const pool = createMockPool(defaultHandlers({
      firstSendAt: daysAgo(90),
      recentSends: 0,
      bounces: 0,
      opens: 0,
      replies: 0,
    }));

    const snapshot = await buildInboxSnapshot(21, { pool, now: NOW, warmupStages: [] });

    assert.equal(snapshot.inboxAgeDays, 90);
    assert.equal(snapshot.inboxAgeSource, 'first_send');
    assert.equal(snapshot.recentSends, 0);
    assert.equal(snapshot.bounceRate, 0);
    assert.equal(snapshot.openRate, 0);
    assert.equal(snapshot.replyRate, 0);
  });

  it('uses warmup_start_date when no send history exists', async () => {
    const pool = createMockPool(defaultHandlers({
      firstSendAt: null,
      warmupStartDate: daysAgo(45),
      recentSends: 0,
    }));

    const snapshot = await buildInboxSnapshot(21, { pool, now: NOW, warmupStages: [] });

    assert.equal(snapshot.inboxAgeSource, 'warmup_start_date');
    assert.equal(snapshot.inboxAgeDays, 45);
  });

  it('uses created_at when no send history or warmup date exists', async () => {
    const pool = createMockPool(defaultHandlers({
      firstSendAt: null,
      warmupStartDate: null,
      createdAt: daysAgo(60),
      recentSends: 0,
    }));

    const snapshot = await buildInboxSnapshot(21, { pool, now: NOW, warmupStages: [] });

    assert.equal(snapshot.inboxAgeSource, 'created_at');
    assert.equal(snapshot.inboxAgeDays, 60);
  });

  it('does not alter 7-day reputation rates when age is sourced all-time', async () => {
    const pool = createMockPool(defaultHandlers({
      firstSendAt: daysAgo(90),
      recentSends: 40,
      bounces: 2,
      opens: 20,
      replies: 4,
      complaints: 1,
    }));

    const snapshot = await buildInboxSnapshot(21, { pool, now: NOW, warmupStages: [] });

    assert.equal(snapshot.inboxAgeDays, 90);
    assert.equal(snapshot.recentSends, 40);
    assert.equal(snapshot.bounceRate, 0.05);
    assert.equal(snapshot.openRate, 0.5);
    assert.equal(snapshot.replyRate, 0.1);
    assert.equal(snapshot.complaintRate, 0.025);
    assert.equal(snapshot.sentToday, 0);
    assert.equal(snapshot.sentYesterday, 18);
    assert.equal(snapshot.historicalDailyAvg, 16);
  });

  it('reconstructs age from DB queries alone with no prior in-memory snapshot', async () => {
    const handlers = defaultHandlers({
      firstSendAt: daysAgo(47),
      recentSends: 12,
    });
    const pool = createMockPool(handlers);

    const first = await buildInboxSnapshot(21, { pool, now: NOW, warmupStages: [] });
    const second = await buildInboxSnapshot(21, { pool, now: NOW, warmupStages: [] });

    assert.deepEqual(
      {
        inboxAgeDays: first.inboxAgeDays,
        inboxAgeSource: first.inboxAgeSource,
        recentSends: first.recentSends,
        bounceRate: first.bounceRate,
      },
      {
        inboxAgeDays: second.inboxAgeDays,
        inboxAgeSource: second.inboxAgeSource,
        recentSends: second.recentSends,
        bounceRate: second.bounceRate,
      }
    );
    assert.ok(pool.queries.some((q) => /MIN\(event_at\)/.test(q.sql)));
    assert.ok(pool.queries.some((q) => /INTERVAL '7 days'/.test(q.sql)));
  });
});
