'use strict';
// Opt-in distribution mode must install a durable, deployment-wide reservation
// implementation. No fallback that silently resets quota on process restart.
let configured = null;
function configureSharedRequestBudget(budget) {
  if (!budget || typeof budget.remainingMs !== 'function' || typeof budget.shouldApply !== 'function' || typeof budget.consume !== 'function' || typeof budget.backoff !== 'function') {
    throw new Error('distribution_budget_invalid');
  }
  configured = budget;
}
const sharedRequestBudget = {
  async shouldApply(lane) {
    if (!configured) return false;
    const active = await configured.shouldApply(lane);
    if (typeof active !== 'boolean') throw new Error('distribution_activation_state_unknown');
    return active;
  },
  async remainingMs(lane) {
    if (!configured) return 0;
    const remaining = await configured.remainingMs(lane);
    if (!Number.isFinite(remaining) || remaining < 0) throw new Error('distribution_activation_deadline_unknown');
    return remaining;
  },
  async consume(lane) { return configured ? configured.consume(lane) : false; },
  async backoff(untilMs) { if (configured) await configured.backoff(untilMs); },
};
// Configuration alone is not evidence that the DB-clock activation window is open.
function isSharedRequestBudgetActive() { return configured !== null; }
function budgetError() { return new Error('distribution_request_budget_unavailable'); }
module.exports = { configureSharedRequestBudget, isSharedRequestBudgetActive, sharedRequestBudget, budgetError };
