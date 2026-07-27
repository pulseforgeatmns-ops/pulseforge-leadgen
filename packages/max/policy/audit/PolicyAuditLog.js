'use strict';

/**
 * Immutable policy audit trail (append-only in-memory store).
 *
 * PolicyAudit {
 *   timestamp, recommendationId, decision, matchedRules, operator
 * }
 */
class PolicyAuditLog {
  constructor() {
    /** @type {object[]} */
    this._rows = [];
    this._seq = 0;
  }

  /**
   * @param {object} input
   */
  record(input) {
    if (!input || !input.decision) {
      throw new Error('PolicyAuditLog.record requires decision');
    }
    this._seq += 1;
    const row = Object.freeze({
      id: `policy-audit:${String(input.tenantId || 'unknown')}:${String(this._seq).padStart(8, '0')}`,
      timestamp: input.timestamp || new Date().toISOString(),
      tenantId: input.tenantId != null ? String(input.tenantId) : null,
      recommendationId: input.recommendationId || null,
      decision: Object.freeze({
        allowed: Boolean(input.decision.allowed),
        requiresApproval: Boolean(input.decision.requiresApproval),
        blocked: Boolean(input.decision.blocked),
        severity: String(input.decision.severity || 'none'),
        outcome: String(input.decision.outcome || ''),
        reason: String(input.decision.reason || ''),
      }),
      matchedRules: Object.freeze(
        (input.matchedRules || []).map((r) =>
          Object.freeze({
            ruleId: r.ruleId,
            action: r.action,
            severity: r.severity,
            reason: r.reason,
          })
        )
      ),
      operator: input.operator != null ? String(input.operator) : null,
      meta: Object.freeze({ ...(input.meta || {}) }),
    });
    this._rows.push(row);
    return row;
  }

  /**
   * @param {{ tenantId?: string, recommendationId?: string, limit?: number }} [filter]
   */
  list(filter = {}) {
    let rows = this._rows.slice();
    if (filter.tenantId != null) {
      rows = rows.filter((r) => r.tenantId === String(filter.tenantId));
    }
    if (filter.recommendationId != null) {
      rows = rows.filter(
        (r) => r.recommendationId === String(filter.recommendationId)
      );
    }
    rows.sort((a, b) => {
      const t = String(a.timestamp).localeCompare(String(b.timestamp));
      if (t !== 0) return t;
      return String(a.id).localeCompare(String(b.id));
    });
    if (filter.limit != null) {
      rows = rows.slice(-Number(filter.limit));
    }
    return rows.map((r) => ({ ...r, decision: { ...r.decision }, matchedRules: r.matchedRules.map((m) => ({ ...m })), meta: { ...r.meta } }));
  }

  count(tenantId) {
    if (tenantId == null) return this._rows.length;
    return this._rows.filter((r) => r.tenantId === String(tenantId)).length;
  }

  clear() {
    this._rows = [];
    this._seq = 0;
  }
}

module.exports = {
  PolicyAuditLog,
};
