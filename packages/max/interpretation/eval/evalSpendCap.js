'use strict';

/**
 * In-process spend cap for the offline interpretation evaluation.
 *
 * The evaluation must stay offline from production databases, so it cannot
 * use the shared spend ledger (utils/apiSpendControl writes api_usage_events
 * through db.js). This cap plugs into the existing utils/meteredAnthropic
 * meterClient as its reserve/settle pair, so pricing still comes from
 * utils/apiPricing and every attempt is still reserved before it is sent.
 *
 * Fail-closed rules:
 *   - an unpriced request (model absent from apiPricing) is refused, because a
 *     cap that cannot price a call cannot bound it;
 *   - a reservation that would take committed spend past maxUsd is refused and
 *     marks the cap exhausted, so the runner stops instead of scoring the
 *     resulting degraded frames as if they were model output;
 *   - a failed or uncertain call keeps its full reservation.
 */

const { randomUUID } = require('node:crypto');

class EvalSpendCapError extends Error {
  constructor(code) {
    super(code);
    this.code = code;
  }
}

function createEvalSpendCap({ maxUsd } = {}) {
  if (typeof maxUsd !== 'number' || !Number.isFinite(maxUsd) || maxUsd <= 0) {
    throw new EvalSpendCapError('eval_cost_cap_required');
  }
  const open = new Map();
  const state = { maxUsd, settledUsd: 0, attempts: 0, settled: 0, uncertain: 0, refused: null };

  const committed = () => state.settledUsd + [...open.values()].reduce((a, b) => a + b, 0);

  async function reserve(metadata) {
    if (state.refused) throw new EvalSpendCapError(state.refused);
    const estimate = metadata && metadata.estimatedUsd;
    if (estimate == null || !Number.isFinite(Number(estimate)) || Number(estimate) < 0) {
      state.refused = 'eval_unpriced_request';
      throw new EvalSpendCapError(state.refused);
    }
    if (committed() + Number(estimate) > maxUsd) {
      state.refused = 'eval_cost_cap_reached';
      throw new EvalSpendCapError(state.refused);
    }
    const id = randomUUID();
    open.set(id, Number(estimate));
    state.attempts += 1;
    return id;
  }

  async function settle(id, outcome = {}) {
    const reserved = open.get(id);
    if (reserved === undefined) return;
    open.delete(id);
    const actual = outcome.status === 'ok' && outcome.estimatedUsd != null && Number.isFinite(Number(outcome.estimatedUsd))
      ? Number(outcome.estimatedUsd)
      : reserved; // unknown actual: keep the conservative reservation
    if (outcome.status !== 'ok') state.uncertain += 1;
    state.settled += 1;
    state.settledUsd += actual;
  }

  return {
    reserve,
    settle,
    get exhausted() { return state.refused !== null; },
    snapshot() {
      return {
        maxUsd,
        committedUsd: Math.round(committed() * 1e6) / 1e6,
        settledUsd: Math.round(state.settledUsd * 1e6) / 1e6,
        attempts: state.attempts,
        settled: state.settled,
        uncertain: state.uncertain,
        openReservations: open.size,
        refused: state.refused,
      };
    },
  };
}

module.exports = { createEvalSpendCap, EvalSpendCapError };
