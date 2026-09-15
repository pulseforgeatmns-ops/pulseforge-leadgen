'use strict';

/**
 * SPEC-255 / SPEC-254 — canonical Emmett capacity envelope for tenant outreach.
 * Single authority path: evidence → Emmett → envelope → authorization → send.
 */

const { scoreInboxHealth } = require('./InboxHealth');
const { recommendCapacity } = require('./Capacity');
const { evaluateGovernor } = require('./Governor');
const { authenticationFromVerificationState } = require('./AuthEvidence');
const { BOOTSTRAP_MODE } = require('./Bootstrap');

function buildCapacityEnvelope(input = {}) {
  const snapshot = { ...(input.snapshot || {}) };
  if (input.verificationState && !input.snapshot?.authentication?.provenance) {
    snapshot.authentication = authenticationFromVerificationState(input.verificationState);
  }

  const health = input.health || scoreInboxHealth(snapshot);
  const capacity = input.capacity || recommendCapacity(snapshot, health);
  const governor = input.governor || evaluateGovernor(snapshot, health, capacity);

  const sentToday = Number(input.sentToday ?? snapshot.sentToday ?? 0);
  const scheduledToday = Number(input.scheduledToday ?? snapshot.scheduledToday ?? 0);
  const limit = governor.halt
    ? 0
    : Math.min(
      Number(capacity.recommended || 0),
      governor.slowCap != null ? governor.slowCap : Number(capacity.recommended || 0)
    );
  const remaining = Math.max(0, limit - sentToday - scheduledToday);

  return {
    kind: 'capacity_envelope',
    spec: 'SPEC-255',
    tenantId: snapshot.tenantId || input.tenantId || null,
    clientId: snapshot.clientId || input.clientId || null,
    sendingIdentityId: snapshot.sendingIdentityId || input.sendingIdentityId || null,
    mailboxIntegrationId: snapshot.mailboxIntegrationId || input.mailboxIntegrationId || null,
    localDate: snapshot.localDate || input.localDate || null,
    timeZone: snapshot.timeZone || input.timeZone || 'America/New_York',
    mode: capacity.mode || 'normal',
    recommended: Number(capacity.recommended || 0),
    ceiling: Number(capacity.ceiling || 0),
    confidence: capacity.confidence ?? null,
    outlook: capacity.outlook || null,
    statement: capacity.statement || null,
    authentication: snapshot.authentication || {},
    bootstrap: capacity.bootstrap || { active: false },
    health: {
      score: health.score,
      label: health.label,
      reasons: health.reasons || [],
    },
    governor: {
      outcome: governor.outcome,
      reason: governor.reason,
      halt: governor.halt === true,
      slowCap: governor.slowCap ?? null,
    },
    accounting: {
      sentToday,
      scheduledToday,
      remaining,
      limit,
    },
    spacing: capacity.bootstrap?.active
      ? {
        minSpacingMinutes: capacity.bootstrap.minSpacingMinutes,
        businessHours: capacity.bootstrap.businessHours,
      }
      : null,
    decisiveReasoning: buildDecisiveReasoning(snapshot, health, capacity, governor),
  };
}

function buildDecisiveReasoning(snapshot, health, capacity, governor) {
  const lines = [];
  if (capacity.mode === BOOTSTRAP_MODE) {
    lines.push('Normal SPEC-117 capacity rounded to zero or insufficient; bootstrap allowance applied.');
  }
  if (capacity.bootstrap?.active) {
    lines.push(`Bootstrap allowance ${capacity.recommended} (warm-up ceiling ${capacity.ceiling}).`);
  } else if (Number(capacity.recommended || 0) <= 0) {
    lines.push('Recommended capacity is zero — governor should halt.');
  } else {
    lines.push(`Normal capacity recommendation ${capacity.recommended}.`);
  }
  lines.push(`Governor: ${governor.outcome} — ${governor.reason}`);
  lines.push(`Health score: ${health.score}`);
  if (snapshot.inboxAgeSource) {
    lines.push(`Inbox age ${snapshot.inboxAgeDays}d (source: ${snapshot.inboxAgeSource}).`);
  }
  if (snapshot.warmup?.status) {
    lines.push(`Warm-up: ${snapshot.warmup.status}, active send days ${snapshot.warmup.activeSendDays ?? 0}.`);
  }
  return lines;
}

module.exports = {
  buildCapacityEnvelope,
  buildDecisiveReasoning,
};
