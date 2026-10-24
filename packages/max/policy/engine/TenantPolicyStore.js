'use strict';

const { DEFAULT_TENANT_POLICY } = require('../PolicyTypes');

/**
 * Tenant policy configuration store — data-driven, no recompile to change policy.
 * Deep-freezes returned configs so rules cannot mutate tenant policy.
 */
class TenantPolicyStore {
  constructor(options = {}) {
    /** @type {Map<string, object>} */
    this._byTenant = new Map();
    this._defaults = freezePolicy({
      ...DEFAULT_TENANT_POLICY,
      ...(options.defaults || {}),
    });
  }

  /**
   * @param {string} tenantId
   * @param {object} config
   */
  set(tenantId, config) {
    if (!tenantId) throw new Error('TenantPolicyStore.set requires tenantId');
    if (!config || typeof config !== 'object') {
      throw new Error('TenantPolicyStore.set requires config object');
    }
    const merged = freezePolicy({
      ...DEFAULT_TENANT_POLICY,
      ...clonePlain(config),
    });
    this._byTenant.set(String(tenantId), merged);
    return merged;
  }

  /**
   * @param {string} tenantId
   */
  get(tenantId) {
    const id = String(tenantId);
    if (this._byTenant.has(id)) {
      return this._byTenant.get(id);
    }
    return this._defaults;
  }

  /**
   * @param {string} tenantId
   */
  has(tenantId) {
    return this._byTenant.has(String(tenantId));
  }

  /**
   * @param {string} tenantId
   */
  delete(tenantId) {
    return this._byTenant.delete(String(tenantId));
  }

  clear() {
    this._byTenant.clear();
  }

  listTenantIds() {
    return [...this._byTenant.keys()].sort();
  }
}

/**
 * @param {object} config
 */
function freezePolicy(config) {
  const out = {
    minimumConfidence: Number(config.minimumConfidence),
    maximumRisk: Number(config.maximumRisk),
    maximumContradictionSeverity: Number(
      config.maximumContradictionSeverity != null
        ? config.maximumContradictionSeverity
        : DEFAULT_TENANT_POLICY.maximumContradictionSeverity
    ),
    approvalRequired: Object.freeze(
      [...(config.approvalRequired || [])].map(String).sort()
    ),
    blockedDays: Object.freeze(
      [...(config.blockedDays || [])].map(String)
    ),
    blockAutonomousOutreach:
      config.blockAutonomousOutreach !== undefined
        ? Boolean(config.blockAutonomousOutreach)
        : true,
    cooldownHours:
      config.cooldownHours == null ? 24 : Number(config.cooldownHours),
    requireVerifiedDecisionMakerFor: Object.freeze(
      [...(config.requireVerifiedDecisionMakerFor || [])].map(String).sort()
    ),
    maxEvidenceAgeDays:
      config.maxEvidenceAgeDays == null
        ? 90
        : Number(config.maxEvidenceAgeDays),
    dailyOutreachLimit:
      config.dailyOutreachLimit == null
        ? null
        : Number(config.dailyOutreachLimit),
  };
  return Object.freeze(out);
}

function clonePlain(value) {
  return JSON.parse(JSON.stringify(value));
}

module.exports = {
  TenantPolicyStore,
  freezePolicy,
  DEFAULT_TENANT_POLICY,
};
