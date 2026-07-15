const assert = require('node:assert/strict');
const test = require('node:test');
const { loadMaxOrchestrationConfig } = require('../config/maxOrchestration');

test('Max orchestration is fail-closed and shadow-on by default', () => {
  const config = loadMaxOrchestrationConfig({ env: {} });
  assert.equal(config.enabled, false);
  assert.equal(config.flags.max_scoring_enabled, false);
  assert.equal(config.flags.max_shadow_mode, true);
  assert.equal(config.autonomy.auto_start_warm_sequence, false);
});

test('environment flags override client configuration', () => {
  const config = loadMaxOrchestrationConfig({
    env: { MAX_ORCHESTRATION_ENABLED: 'true', MAX_SCORING_ENABLED: 'false' },
    clientOverrides: { flags: { max_scoring_enabled: true }, thresholds: { warm: 65 } },
  });
  assert.equal(config.enabled, true);
  assert.equal(config.flags.max_scoring_enabled, false);
  assert.equal(config.thresholds.warm, 65);
});

test('unsafe non-shadow transition configuration is rejected in Phase 2', () => {
  assert.throws(() => loadMaxOrchestrationConfig({
    env: { MAX_SHADOW_MODE: 'false', MAX_STATE_TRANSITIONS_ENABLED: 'true' },
  }), /Phase 2 requires/);
});

test('equivalent prospect-facing flags are explicit and fail closed by default', () => {
  const defaults = loadMaxOrchestrationConfig({ env: {} });
  for (const flag of [
    'max_sequence_actions_enabled',
    'max_operator_tasks_enabled',
    'max_enrichment_retry_enabled',
    'max_prospect_actions_enabled',
  ]) assert.equal(defaults.flags[flag], false, flag);

  const configured = loadMaxOrchestrationConfig({
    env: {
      MAX_SEQUENCE_ACTIONS_ENABLED: 'true',
      MAX_OPERATOR_TASKS_ENABLED: 'true',
      MAX_ENRICHMENT_RETRY_ENABLED: 'true',
      MAX_PROSPECT_ACTIONS_ENABLED: 'true',
    },
  });
  assert.equal(configured.flags.max_sequence_actions_enabled, true);
  assert.equal(configured.flags.max_operator_tasks_enabled, true);
  assert.equal(configured.flags.max_enrichment_retry_enabled, true);
  assert.equal(configured.flags.max_prospect_actions_enabled, true);
});
