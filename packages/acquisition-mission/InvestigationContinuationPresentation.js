'use strict';

/**
 * SPEC-203 — Investigation Continuation Presentation Contract.
 * Projects operator-facing deltas from committed discovery payloads (before vs after),
 * never from independently reconstructed Scout state.
 */

const { SPECIALISTS, CONTRIBUTION_KINDS } = require('./types');
const { presentationFromDiscoveryPayload } = require('./DiscoveryPresentation');
const { evaluatePrioritizationReadiness } = require('./DecisionReadiness');
const { normalizeProviderExecution } = require('../scout/coverage/ProviderExecution');
const { QUALIFICATION_STATUSES } = require('../max/scoutAcquisition/Types');
const { TASK_STATUS } = require('../scout/investigation/CandidateInvestigation');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function candidateKey(row = {}) {
  return (
    asText(
      row.candidateId ||
        row.candidate_id ||
        row.canonicalIdentity ||
        row.id ||
        row.companyId ||
        row.placeId
    ) || asText(row.name)
  ).toLowerCase();
}

function candidateQualificationStatus(row = {}) {
  const qual = row.qualification || {};
  if (qual.status) return String(qual.status);
  if (row.qualificationStatus) return String(row.qualificationStatus);
  if (row.excluded === true || row.prospectBucket === 'excluded') {
    return QUALIFICATION_STATUSES.NOT_QUALIFIED;
  }
  if (row.qualified === true) return QUALIFICATION_STATUSES.QUALIFIED;
  return QUALIFICATION_STATUSES.UNCERTAIN;
}

function candidateQualificationReason(row = {}) {
  const qual = row.qualification || {};
  return (
    qual.reason ||
    qual.rejectedBecause ||
    row.qualificationReason ||
    row.excludedReason ||
    null
  );
}

function evidenceIdentity(item) {
  if (item == null) return '';
  if (typeof item === 'string') return item.trim().toLowerCase();
  return [
    asText(item.id),
    asText(item.label),
    asText(item.company || item.snapshot?.companyName),
    asText(item.source),
  ]
    .filter(Boolean)
    .join('|')
    .toLowerCase();
}

function collectEvidenceItems(payload = {}) {
  const items = [];
  if (Array.isArray(payload.evidence)) items.push(...payload.evidence);
  if (Array.isArray(payload.evidenceRaw)) items.push(...payload.evidenceRaw);
  for (const row of payload.candidateUniverse || []) {
    if (Array.isArray(row.evidence)) items.push(...row.evidence);
    if (Array.isArray(row.evidenceRefs)) items.push(...row.evidenceRefs);
  }
  return items;
}

function formatEvidenceAddedLine(item) {
  if (item == null) return '';
  if (typeof item === 'string') return item.trim();
  const company =
    item.company ||
    item.snapshot?.companyName ||
    item.entityId ||
    null;
  const label = item.label || item.text || item.source || 'evidence';
  const gap =
    item.evidenceType ||
    item.gap ||
    item.kind ||
    item.sourceType ||
    null;
  if (company) {
    const suffix = gap ? ` — ${String(gap).replace(/_/g, ' ')} evidence` : '';
    return `${company}${suffix || ` — ${label}`}`;
  }
  return label;
}

function indexCandidates(payload = {}) {
  const map = new Map();
  const rows = Array.isArray(payload.candidateUniverse) ? payload.candidateUniverse : [];
  for (const row of rows) {
    const key = candidateKey(row);
    if (!key) continue;
    map.set(key, row);
  }
  return map;
}

function resolveCandidateUniverseCount(payload = {}, presentation = null) {
  if (payload.candidateUniverseCount != null) return Number(payload.candidateUniverseCount);
  if (presentation && presentation.candidateUniverseCount != null) {
    return Number(presentation.candidateUniverseCount);
  }
  if (Array.isArray(payload.candidateUniverse)) return payload.candidateUniverse.length;
  return null;
}

function resolveQualifiedCount(payload = {}, presentation = null) {
  if (payload.qualifiedCount != null) return Number(payload.qualifiedCount);
  if (presentation && presentation.qualifiedCount != null) return Number(presentation.qualifiedCount);
  return 0;
}

function listScoutDiscoveryContributions(contributions = []) {
  return contributions.filter(
    (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
  );
}

/**
 * Resolve before/after discovery payloads from committed mission state.
 * @param {{ snapshot?: object, executionResult?: object }} input
 */
function resolveInvestigationContinuationPayloads(input = {}) {
  const snapshot = input.snapshot || {};
  const executionResult = input.executionResult || {};
  const contributions = snapshot.contributions || [];
  const discoveries = listScoutDiscoveryContributions(contributions);

  let currentPayload =
    (executionResult.discovery && executionResult.discovery.payload) ||
    (discoveries.length ? discoveries[discoveries.length - 1].payload : null) ||
    {};

  let priorPayload = null;
  if (discoveries.length >= 2) {
    priorPayload = discoveries[discoveries.length - 2].payload || {};
  } else if (executionResult.priorDiscoveryPayload) {
    priorPayload = executionResult.priorDiscoveryPayload;
  } else {
    priorPayload = {};
  }

  return { priorPayload, currentPayload };
}

function diffQualificationChanges(priorPayload = {}, currentPayload = {}) {
  const prior = indexCandidates(priorPayload);
  const current = indexCandidates(currentPayload);
  const newlyQualified = [];
  const disqualified = [];
  const stillUncertain = [];

  for (const [key, row] of current.entries()) {
    const before = prior.get(key);
    const afterStatus = candidateQualificationStatus(row);
    const beforeStatus = before ? candidateQualificationStatus(before) : QUALIFICATION_STATUSES.UNCERTAIN;
    const name = row.name || key;

    if (
      afterStatus === QUALIFICATION_STATUSES.QUALIFIED &&
      beforeStatus !== QUALIFICATION_STATUSES.QUALIFIED
    ) {
      newlyQualified.push({ name, id: row.candidateId || row.id || null });
      continue;
    }

    if (
      afterStatus === QUALIFICATION_STATUSES.NOT_QUALIFIED &&
      beforeStatus !== QUALIFICATION_STATUSES.NOT_QUALIFIED
    ) {
      disqualified.push({
        name,
        id: row.candidateId || row.id || null,
        reason: candidateQualificationReason(row) || 'Evidence no longer supports qualification.',
      });
      continue;
    }

    if (afterStatus === QUALIFICATION_STATUSES.UNCERTAIN) {
      stillUncertain.push({ name, id: row.candidateId || row.id || null });
    }
  }

  return { newlyQualified, disqualified, stillUncertain };
}

function diffEvidenceAdded(priorPayload = {}, currentPayload = {}) {
  const priorKeys = new Set(collectEvidenceItems(priorPayload).map(evidenceIdentity).filter(Boolean));
  const added = [];
  const seen = new Set();

  for (const item of collectEvidenceItems(currentPayload)) {
    const key = evidenceIdentity(item);
    if (!key || priorKeys.has(key) || seen.has(key)) continue;
    seen.add(key);
    const line = formatEvidenceAddedLine(item);
    if (line) added.push(line);
  }

  return added;
}

function resolveCandidateInvestigationSource(executionResult = {}, currentPayload = {}) {
  if (currentPayload.candidateInvestigation) return currentPayload.candidateInvestigation;
  const scoutResult = executionResult.scoutResult || {};
  const intelligence = scoutResult.intelligenceResult || scoutResult;
  return (
    intelligence.payload?.candidateInvestigation ||
    scoutResult.payload?.candidateInvestigation ||
    null
  );
}

function summarizeInvestigationTasks(candidateInvestigation) {
  const completed = [];
  const blocked = [];
  if (!candidateInvestigation) return { completed, blocked };

  for (const row of candidateInvestigation.executedTasks || []) {
    const task = row.task || row;
    const candidateName = task.candidateName || task.entityId || task.candidateId || 'Candidate';
    const gap = task.gap || task.evidenceType || 'investigation';
    completed.push(`${candidateName} — ${String(gap).replace(/_/g, ' ')}`);
  }

  for (const task of candidateInvestigation.queue || []) {
    if (task.status !== TASK_STATUS.BLOCKED && task.status !== TASK_STATUS.EXHAUSTED) continue;
    const providers = (task.providers || []).map((p) => p.label || p.id || p.providerId).filter(Boolean);
    const label = providers.length ? providers.join(', ') : task.evidenceType || task.gap || 'provider';
    blocked.push(`${String(label).replace(/_/g, ' ')} unavailable`);
  }

  return {
    completed: [...new Set(completed)],
    blocked: [...new Set(blocked)],
  };
}

function summarizeBlockedProviders(currentPayload = {}, executionResult = {}) {
  const records = normalizeProviderExecution(
    currentPayload.providerExecution ||
      resolveCandidateInvestigationSource(executionResult, currentPayload)?.providerExecution ||
      []
  );
  const blocked = [];
  for (const row of records) {
    if (row.succeeded !== false && row.executed !== false && !row.reason) continue;
    const label = row.providerLabel || row.providerId || row.source || 'Provider';
    if (/unavailable|denied|blocked|failed|error/i.test(String(row.reason || row.googleError || ''))) {
      blocked.push(`${label} unavailable`);
    } else if (row.succeeded === false || row.executed === false) {
      blocked.push(`${label} unavailable`);
    }
  }
  return [...new Set(blocked)];
}

function summarizeProviderResults(currentPayload = {}) {
  const records = normalizeProviderExecution(currentPayload.providerExecution || []);
  return records
    .filter((row) => row.succeeded !== false && (row.results > 0 || row.mappedCandidateCount > 0))
    .map((row) => {
      const label = row.providerLabel || row.providerId || row.source || 'Provider';
      const count = row.results != null ? row.results : row.mappedCandidateCount;
      return count != null ? `${label}: ${count} result${count === 1 ? '' : 's'}` : label;
    });
}

/**
 * Build investigation continuation presentation from committed before/after payloads.
 * @param {object} input
 */
function presentationFromInvestigationContinuation(input = {}) {
  const priorPayload = input.priorPayload || {};
  const currentPayload = input.currentPayload || {};
  const executionResult = input.executionResult || {};
  const mission = input.mission || {};

  const priorPresentation = presentationFromDiscoveryPayload(priorPayload);
  const currentPresentation = presentationFromDiscoveryPayload(currentPayload);

  const candidateUniverseBefore = resolveCandidateUniverseCount(priorPayload, priorPresentation);
  const candidateUniverseAfter = resolveCandidateUniverseCount(currentPayload, currentPresentation);
  const qualifiedBefore = resolveQualifiedCount(priorPayload, priorPresentation);
  const qualifiedAfter = resolveQualifiedCount(currentPayload, currentPresentation);
  const confidenceBefore =
    priorPresentation.confidence != null ? priorPresentation.confidence : mission.confidence;
  const confidenceAfter =
    currentPresentation.confidence != null
      ? currentPresentation.confidence
      : mission.confidence;

  const qualificationChanges = diffQualificationChanges(priorPayload, currentPayload);
  const evidenceAdded = diffEvidenceAdded(priorPayload, currentPayload);
  const candidateInvestigation = resolveCandidateInvestigationSource(executionResult, currentPayload);
  const taskSummary = summarizeInvestigationTasks(candidateInvestigation);
  const blockedProviders = summarizeBlockedProviders(currentPayload, executionResult);
  const blockedInvestigation = [...new Set([...taskSummary.blocked, ...blockedProviders])];
  const providerResults = summarizeProviderResults(currentPayload);

  const readiness = evaluatePrioritizationReadiness(currentPayload);
  const pending = mission.pendingOperatorDecision || {};
  const nextStep = readiness.sufficient
    ? 'Review discovered prospects and approve prioritization to continue.'
    : pending.reason || pending.waitingOn || 'More evidence is still required before prioritization.';
  const operatorDecision = readiness.sufficient
    ? 'Approve prioritization?'
    : pending.prompt || 'Continue investigation?';

  return {
    candidateUniverseBefore,
    candidateUniverseAfter,
    qualifiedBefore,
    qualifiedAfter,
    confidenceBefore,
    confidenceAfter,
    evidenceAdded,
    qualificationChanges,
    stillUncertain: qualificationChanges.stillUncertain,
    investigationTasksCompleted: taskSummary.completed,
    blockedInvestigation,
    providerResults,
    nextStep,
    operatorDecision,
    sufficientForPrioritization: readiness.sufficient,
    currentPresentation,
  };
}

/**
 * @param {ReturnType<typeof presentationFromInvestigationContinuation>} presentation
 * @returns {string[]}
 */
function formatInvestigationContinuationLines(presentation) {
  if (!presentation) return [];
  const lines = ['Investigation Continued', ''];

  lines.push('Candidate Universe');
  lines.push('');
  if (
    presentation.candidateUniverseBefore != null &&
    presentation.candidateUniverseAfter != null
  ) {
    lines.push(`${presentation.candidateUniverseBefore} → ${presentation.candidateUniverseAfter}`);
  } else if (presentation.candidateUniverseAfter != null) {
    lines.push(String(presentation.candidateUniverseAfter));
  } else {
    lines.push('—');
  }
  lines.push('');

  lines.push('Qualified');
  lines.push('');
  lines.push(`${presentation.qualifiedBefore} → ${presentation.qualifiedAfter}`);
  lines.push('');

  if (presentation.confidenceBefore != null || presentation.confidenceAfter != null) {
    lines.push('Confidence');
    lines.push('');
    const before =
      presentation.confidenceBefore != null
        ? Number(presentation.confidenceBefore).toFixed(2)
        : '—';
    const after =
      presentation.confidenceAfter != null
        ? Number(presentation.confidenceAfter).toFixed(2)
        : '—';
    lines.push(`${before} → ${after}`);
    lines.push('');
  }

  lines.push('Evidence Added');
  lines.push('');
  if (presentation.evidenceAdded.length) {
    for (const item of presentation.evidenceAdded) {
      lines.push(`• ${item}`);
    }
  } else {
    lines.push('• No new attributable evidence recorded.');
  }
  lines.push('');

  lines.push('Qualification Changes');
  lines.push('');
  lines.push(`• ${presentation.qualificationChanges.newlyQualified.length} newly qualified`);
  if (presentation.qualificationChanges.newlyQualified.length) {
    for (const row of presentation.qualificationChanges.newlyQualified) {
      lines.push(`  - ${row.name}`);
    }
  }
  lines.push(`• ${presentation.qualificationChanges.disqualified.length} disqualified`);
  for (const row of presentation.qualificationChanges.disqualified) {
    lines.push(`  - ${row.name}${row.reason ? ` — ${row.reason}` : ''}`);
  }
  if (presentation.stillUncertain.length) {
    lines.push(`• ${presentation.stillUncertain.length} still uncertain`);
    for (const row of presentation.stillUncertain.slice(0, 5)) {
      lines.push(`  - ${row.name}`);
    }
  }
  lines.push('');

  if (presentation.investigationTasksCompleted.length) {
    lines.push('Investigation Tasks Completed');
    lines.push('');
    for (const item of presentation.investigationTasksCompleted) {
      lines.push(`• ${item}`);
    }
    lines.push('');
  }

  if (presentation.blockedInvestigation.length) {
    lines.push('Blocked Investigation');
    lines.push('');
    for (const item of presentation.blockedInvestigation) {
      lines.push(`• ${item}`);
    }
    lines.push('');
  }

  if (presentation.providerResults.length) {
    lines.push('Provider Results');
    lines.push('');
    for (const item of presentation.providerResults) {
      lines.push(`• ${item}`);
    }
    lines.push('');
  }

  if (presentation.nextStep) {
    lines.push('Next Step');
    lines.push('');
    lines.push(presentation.nextStep);
    lines.push('');
  }

  return lines;
}

function formatInvestigationContinuationProse(presentation) {
  return formatInvestigationContinuationLines(presentation).join('\n').trim();
}

module.exports = {
  resolveInvestigationContinuationPayloads,
  presentationFromInvestigationContinuation,
  formatInvestigationContinuationLines,
  formatInvestigationContinuationProse,
  candidateQualificationStatus,
  diffEvidenceAdded,
  diffQualificationChanges,
};
