'use strict';
// SPEC-SCOUT-P0-005: bounded diagnostics for unchanged research-only holds.
//
// This throttles LOG LINES only. Every scheduler tick still runs the full
// authorization check (grant, revision pin, released allowance) before this
// module is consulted; nothing here can admit, skip or shorten a claim.
//
// An unchanged steady hold (same tenant, reason, grant and revisions) emits
// one initial diagnostic, then one heartbeat per interval carrying the count
// of suppressed repeats. Any change of reason or diagnostic, or any non-steady
// outcome in between, emits immediately and restarts the sequence. State is
// process-local on purpose: a new process is a new deployment, and its first
// hold must always be reported.
const HEARTBEAT_MS = 60 * 60 * 1000;
const STEADY_HOLDS = Object.freeze(new Set([
  'research_only_revision_mismatch',
  'research_only_awaiting_cycle_release',
  'research_only_cycles_exhausted',
]));

function createResearchHoldLogThrottle({ heartbeatMs = HEARTBEAT_MS, now = () => Date.now() } = {}) {
  const last = new Map();
  return function decide(tenantId, result) {
    const tenant = String(tenantId);
    const halted = result?.halted || null;
    if (!STEADY_HOLDS.has(halted)) { last.delete(tenant); return { emit: true, kind: 'event', suppressed: 0 }; }
    const key = `${halted}|${JSON.stringify(result?.researchOnly?.diagnostic || null)}`;
    const at = now(), prior = last.get(tenant);
    if (!prior || prior.key !== key) {
      last.set(tenant, { key, emittedAt: at, suppressed: 0 });
      return { emit: true, kind: 'initial', suppressed: 0 };
    }
    if (at - prior.emittedAt >= heartbeatMs) {
      const suppressed = prior.suppressed;
      last.set(tenant, { key, emittedAt: at, suppressed: 0 });
      return { emit: true, kind: 'heartbeat', suppressed };
    }
    prior.suppressed += 1;
    return { emit: false, kind: 'suppressed', suppressed: prior.suppressed };
  };
}

module.exports = { createResearchHoldLogThrottle, HEARTBEAT_MS, STEADY_HOLDS };
