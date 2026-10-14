'use strict';
// Opt-in distribution mode must install a durable, deployment-wide reservation
// implementation. No fallback that silently resets quota on process restart.
let configured = null;
function configureSharedRequestBudget(budget) {
  if (!budget || typeof budget.consume !== 'function' || typeof budget.backoff !== 'function') {
    throw new Error('distribution_budget_invalid');
  }
  configured = budget;
}
const sharedRequestBudget = {
  async consume(lane) { return configured ? configured.consume(lane) : false; },
  async backoff(untilMs) { if (configured) await configured.backoff(untilMs); },
};
function isSharedRequestBudgetActive() { return configured !== null; }
function budgetError() { return new Error('distribution_request_budget_unavailable'); }
module.exports = { configureSharedRequestBudget, isSharedRequestBudgetActive, sharedRequestBudget, budgetError };
