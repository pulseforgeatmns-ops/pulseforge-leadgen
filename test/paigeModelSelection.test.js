'use strict';

const assert = require('node:assert/strict');
const { test, describe, beforeEach, afterEach } = require('node:test');

const PAIGE_AGENT_PATH = require.resolve('../paigeAgent');
const DB_PATH = require.resolve('../db');
const ANTHROPIC_PATH = require.resolve('@anthropic-ai/sdk');

function loadPaigeAgent({ env = {}, anthropicFactory } = {}) {
  const savedEnv = {};
  for (const key of ['ACTIVE_CLIENT_ID', 'PAIGE_WRITER_MODEL', 'PAIGE_EVALUATOR_MODEL']) {
    savedEnv[key] = process.env[key];
    if (Object.prototype.hasOwnProperty.call(env, key)) {
      if (env[key] == null) delete process.env[key];
      else process.env[key] = env[key];
    }
  }

  delete require.cache[PAIGE_AGENT_PATH];
  delete require.cache[DB_PATH];
  delete require.cache[ANTHROPIC_PATH];

  require.cache[DB_PATH] = {
    id: DB_PATH,
    filename: DB_PATH,
    loaded: true,
    exports: {
      query: async (sql) => {
        const normalized = String(sql).trim();
        if (/SELECT \* FROM clients WHERE id = \$1 AND active = true/i.test(sql)) {
          return { rows: [{
            id: 10,
            name: 'Anchor Cleaning',
            business_name: 'Anchor Cleaning',
            vertical: 'commercial_cleaning',
            city: 'Manchester',
            state: 'NH',
            enabled_agents: ['scout', 'paige'],
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
        if (/^(?:INSERT|UPDATE|ALTER|DELETE|CREATE|DROP|DO)\b/i.test(normalized)) {
          throw new Error(`Unexpected write in model-selection test: ${normalized.slice(0, 60)}`);
        }
        return { rows: [] };
      },
    },
  };

  const modelsUsed = { writer: [], evaluator: [] };
  class FakeAnthropic {
    constructor() {
      this.messages = {
        create: async request => {
          const prompt = request.messages?.[0]?.content || '';
          if (/Score this social media post/i.test(prompt)) {
            modelsUsed.evaluator.push(request.model);
            return { content: [{ text: JSON.stringify({
              specificity: 9,
              originality: 9,
              hook_strength: 9,
              total: 27,
              weak_dimension: 'none',
              reason: 'Specific, grounded, and direct.',
            }) }] };
          }
          modelsUsed.writer.push(request.model);
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

  require.cache[ANTHROPIC_PATH] = {
    id: ANTHROPIC_PATH,
    filename: ANTHROPIC_PATH,
    loaded: true,
    exports: anthropicFactory || FakeAnthropic,
  };

  const paigeAgent = require('../paigeAgent');
  return { paigeAgent, modelsUsed, savedEnv };
}

function restoreEnv(savedEnv) {
  for (const [key, value] of Object.entries(savedEnv)) {
    if (value == null) delete process.env[key];
    else process.env[key] = value;
  }
  delete require.cache[PAIGE_AGENT_PATH];
  delete require.cache[DB_PATH];
  delete require.cache[ANTHROPIC_PATH];
}

describe('Paige writer/evaluator model selection', () => {
  let savedEnv;

  afterEach(() => {
    if (savedEnv) restoreEnv(savedEnv);
  });

  test('resolvePaigeWriterModel defaults to claude-opus-5-5', () => {
    const { paigeAgent } = loadPaigeAgent({ env: { ACTIVE_CLIENT_ID: '10', PAIGE_WRITER_MODEL: null, PAIGE_EVALUATOR_MODEL: null } });
    savedEnv = { ACTIVE_CLIENT_ID: process.env.ACTIVE_CLIENT_ID, PAIGE_WRITER_MODEL: process.env.PAIGE_WRITER_MODEL, PAIGE_EVALUATOR_MODEL: process.env.PAIGE_EVALUATOR_MODEL };
    assert.equal(paigeAgent._test.resolvePaigeWriterModel({}), 'claude-opus-5-5');
    assert.equal(paigeAgent._test.PAIGE_WRITER_MODEL, 'claude-opus-5-5');
  });

  test('resolvePaigeEvaluatorModel defaults to claude-sonnet-4-6', () => {
    const { paigeAgent } = loadPaigeAgent({ env: { ACTIVE_CLIENT_ID: '10', PAIGE_WRITER_MODEL: null, PAIGE_EVALUATOR_MODEL: null } });
    savedEnv = { ACTIVE_CLIENT_ID: process.env.ACTIVE_CLIENT_ID, PAIGE_WRITER_MODEL: process.env.PAIGE_WRITER_MODEL, PAIGE_EVALUATOR_MODEL: process.env.PAIGE_EVALUATOR_MODEL };
    assert.equal(paigeAgent._test.resolvePaigeEvaluatorModel({}), 'claude-sonnet-4-6');
    assert.equal(paigeAgent._test.PAIGE_EVALUATOR_MODEL, 'claude-sonnet-4-6');
  });

  test('PAIGE_WRITER_MODEL overrides writer only', () => {
    const { paigeAgent, modelsUsed } = loadPaigeAgent({
      env: {
        ACTIVE_CLIENT_ID: '10',
        PAIGE_WRITER_MODEL: 'claude-custom-writer',
        PAIGE_EVALUATOR_MODEL: null,
      },
    });
    savedEnv = { ACTIVE_CLIENT_ID: process.env.ACTIVE_CLIENT_ID, PAIGE_WRITER_MODEL: process.env.PAIGE_WRITER_MODEL, PAIGE_EVALUATOR_MODEL: process.env.PAIGE_EVALUATOR_MODEL };
    assert.equal(paigeAgent._test.PAIGE_WRITER_MODEL, 'claude-custom-writer');
    assert.equal(paigeAgent._test.PAIGE_EVALUATOR_MODEL, 'claude-sonnet-4-6');
    return paigeAgent.run({ client_id: 10, dryRun: true, channel: 'linkedin_page', format: 'dialogue' }).then(result => {
      assert.equal(result.success, true);
      assert.ok(modelsUsed.writer.includes('claude-custom-writer'));
      assert.ok(modelsUsed.evaluator.every(model => model === 'claude-sonnet-4-6'));
    });
  });

  test('PAIGE_EVALUATOR_MODEL overrides evaluator only', () => {
    const { paigeAgent, modelsUsed } = loadPaigeAgent({
      env: {
        ACTIVE_CLIENT_ID: '10',
        PAIGE_WRITER_MODEL: null,
        PAIGE_EVALUATOR_MODEL: 'claude-custom-evaluator',
      },
    });
    savedEnv = { ACTIVE_CLIENT_ID: process.env.ACTIVE_CLIENT_ID, PAIGE_WRITER_MODEL: process.env.PAIGE_WRITER_MODEL, PAIGE_EVALUATOR_MODEL: process.env.PAIGE_EVALUATOR_MODEL };
    assert.equal(paigeAgent._test.PAIGE_WRITER_MODEL, 'claude-opus-5-5');
    assert.equal(paigeAgent._test.PAIGE_EVALUATOR_MODEL, 'claude-custom-evaluator');
    return paigeAgent.run({ client_id: 10, dryRun: true, channel: 'linkedin_page', format: 'dialogue' }).then(result => {
      assert.equal(result.success, true);
      assert.ok(modelsUsed.writer.every(model => model === 'claude-opus-5-5'));
      assert.ok(modelsUsed.evaluator.includes('claude-custom-evaluator'));
    });
  });

  test('logs writer and evaluator models once per generation invocation', async () => {
    const { paigeAgent } = loadPaigeAgent({ env: { ACTIVE_CLIENT_ID: '10', PAIGE_WRITER_MODEL: null, PAIGE_EVALUATOR_MODEL: null } });
    savedEnv = { ACTIVE_CLIENT_ID: process.env.ACTIVE_CLIENT_ID, PAIGE_WRITER_MODEL: process.env.PAIGE_WRITER_MODEL, PAIGE_EVALUATOR_MODEL: process.env.PAIGE_EVALUATOR_MODEL };
    const logs = [];
    const originalLog = console.log;
    console.log = (...args) => {
      logs.push(args.join(' '));
      originalLog(...args);
    };
    try {
      await paigeAgent.generateSocialContent({ client_id: 10, dryRun: true, channel: 'linkedin_page', format: 'dialogue' });
    } finally {
      console.log = originalLog;
    }
    assert.equal(logs.filter(line => line === '[Paige] writer_model=claude-opus-5-5').length, 1);
    assert.equal(logs.filter(line => line === '[Paige] evaluator_model=claude-sonnet-4-6').length, 1);
  });
});
