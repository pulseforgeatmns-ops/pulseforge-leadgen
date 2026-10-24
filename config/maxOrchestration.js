const { resolveVerticalTier } = require('../utils/verticalTiers');

const DEFAULT_MAX_ORCHESTRATION_CONFIG = Object.freeze({
  version: 'max-orchestration-v1',
  decision_version: 'max-decision-v1',
  score_version: 'warmth-v1',
  enabled: false,
  flags: Object.freeze({
    max_scoring_enabled: false,
    max_state_transitions_enabled: false,
    max_shadow_mode: true,
    max_enrichment_actions_enabled: false,
    max_warm_sequence_enabled: false,
    max_call_tasks_enabled: false,
    max_hot_escalations_enabled: false,
    max_recycle_actions_enabled: false,
    max_sequence_actions_enabled: false,
    max_operator_tasks_enabled: false,
    max_enrichment_retry_enabled: false,
    max_prospect_actions_enabled: false,
  }),
  thresholds: Object.freeze({ heating: 40, warm: 60, hot: 80 }),
  downgrade_thresholds: Object.freeze({ hot_to_warm: 70, warm_to_heating: 50, heating_to_cold: 30 }),
  downgrade_stabilization_hours: 72,
  signal_windows: Object.freeze({
    human_opens_days: 7,
    click_days: 14,
    company_signal_days: 14,
    icp_delta_days: 7,
    recency_days: 7,
  }),
  scoring: Object.freeze({
    icp_80_plus: 30,
    icp_65_79: 20,
    icp_50_64: 10,
    tier_a_vertical: 10,
    first_human_open: 5,
    second_human_open: 8,
    third_human_open: 12,
    verified_click: 20,
    icp_delta_15: 10,
    icp_delta_40: 15,
    decision_maker: 5,
    verified_email: 5,
    phone_available: 5,
    recent_24h: 10,
    recent_72h: 5,
    recent_7d: 2,
    unverified_email_only: -5,
    repeated_enrichment_failure: -10,
    soft_bounce: -15,
    repeated_enrichment_failure_count: 2,
  }),
  recycle_days: Object.freeze({ cold_completed: 60, warm_completed: 30, not_now: 90 }),
  autonomy: Object.freeze({
    auto_start_warm_sequence: false,
    auto_create_call_tasks: false,
    auto_retry_enrichment: false,
    auto_null_confirmed_invalid_contacts: false,
    pause_cold_on_warm: false,
  }),
});

const FLAG_ENV = Object.freeze({
  max_scoring_enabled: 'MAX_SCORING_ENABLED',
  max_state_transitions_enabled: 'MAX_STATE_TRANSITIONS_ENABLED',
  max_shadow_mode: 'MAX_SHADOW_MODE',
  max_enrichment_actions_enabled: 'MAX_ENRICHMENT_ACTIONS_ENABLED',
  max_warm_sequence_enabled: 'MAX_WARM_SEQUENCE_ENABLED',
  max_call_tasks_enabled: 'MAX_CALL_TASKS_ENABLED',
  max_hot_escalations_enabled: 'MAX_HOT_ESCALATIONS_ENABLED',
  max_recycle_actions_enabled: 'MAX_RECYCLE_ACTIONS_ENABLED',
  max_sequence_actions_enabled: 'MAX_SEQUENCE_ACTIONS_ENABLED',
  max_operator_tasks_enabled: 'MAX_OPERATOR_TASKS_ENABLED',
  max_enrichment_retry_enabled: 'MAX_ENRICHMENT_RETRY_ENABLED',
  max_prospect_actions_enabled: 'MAX_PROSPECT_ACTIONS_ENABLED',
});

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function mergeDeep(base, override) {
  if (!override || typeof override !== 'object' || Array.isArray(override)) return clone(base);
  const result = clone(base);
  for (const [key, value] of Object.entries(override)) {
    if (value && typeof value === 'object' && !Array.isArray(value) && result[key] && typeof result[key] === 'object') {
      result[key] = mergeDeep(result[key], value);
    } else {
      result[key] = value;
    }
  }
  return result;
}

function parseBoolean(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  throw new Error(`Invalid boolean feature flag value: ${value}`);
}

function validateMaxOrchestrationConfig(config) {
  const errors = [];
  const { thresholds, downgrade_thresholds: down, flags, scoring } = config;
  if (!(thresholds.heating < thresholds.warm && thresholds.warm < thresholds.hot)) {
    errors.push('thresholds must increase from heating to warm to hot');
  }
  if (!(down.heating_to_cold < thresholds.heating)) errors.push('heating_to_cold must be below heating threshold');
  if (!(down.warm_to_heating < thresholds.warm)) errors.push('warm_to_heating must be below warm threshold');
  if (!(down.hot_to_warm < thresholds.hot)) errors.push('hot_to_warm must be below hot threshold');
  if (!Number.isFinite(Number(config.downgrade_stabilization_hours)) || Number(config.downgrade_stabilization_hours) < 0) {
    errors.push('downgrade_stabilization_hours must be non-negative');
  }
  for (const [key, value] of Object.entries(flags)) {
    if (typeof value !== 'boolean') errors.push(`flags.${key} must be boolean`);
  }
  for (const [key, value] of Object.entries(scoring)) {
    if (!Number.isFinite(Number(value))) errors.push(`scoring.${key} must be numeric`);
  }
  if (!flags.max_shadow_mode && flags.max_state_transitions_enabled) {
    errors.push('Phase 2 requires max_shadow_mode=true whenever state transitions are enabled');
  }
  if (errors.length) throw new Error(`Invalid Max orchestration configuration: ${errors.join('; ')}`);
  return config;
}

function loadMaxOrchestrationConfig({ env = process.env, clientOverrides = null } = {}) {
  const config = mergeDeep(DEFAULT_MAX_ORCHESTRATION_CONFIG, clientOverrides || {});
  config.enabled = parseBoolean(env.MAX_ORCHESTRATION_ENABLED, config.enabled);
  for (const [flag, envName] of Object.entries(FLAG_ENV)) {
    config.flags[flag] = parseBoolean(env[envName], config.flags[flag]);
  }
  return validateMaxOrchestrationConfig(config);
}

function withProspectTier(prospect, clientConfig = {}) {
  const tier = resolveVerticalTier(prospect?.vertical, clientConfig);
  return { ...prospect, vertical_tier: prospect?.vertical_tier || tier.tier };
}

module.exports = {
  DEFAULT_MAX_ORCHESTRATION_CONFIG,
  FLAG_ENV,
  loadMaxOrchestrationConfig,
  mergeDeep,
  parseBoolean,
  validateMaxOrchestrationConfig,
  withProspectTier,
};
