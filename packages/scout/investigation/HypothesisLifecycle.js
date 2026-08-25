'use strict';

/**
 * SPEC-159 — Hypothesis lifecycle management.
 * Generated → Testing → Supported → Rejected → Archived
 * Nothing disappears. Scout remembers why it stopped believing something.
 */

const HYPOTHESIS_LIFECYCLE = Object.freeze({
  GENERATED: 'generated',
  TESTING: 'testing',
  SUPPORTED: 'supported',
  REJECTED: 'rejected',
  ARCHIVED: 'archived',
});

const SEARCH_STATUS_TO_LIFECYCLE = Object.freeze({
  open: HYPOTHESIS_LIFECYCLE.TESTING,
  confirmed: HYPOTHESIS_LIFECYCLE.SUPPORTED,
  rejected: HYPOTHESIS_LIFECYCLE.REJECTED,
  inconclusive: HYPOTHESIS_LIFECYCLE.ARCHIVED,
});

function mapSearchStatusToLifecycle(status) {
  return SEARCH_STATUS_TO_LIFECYCLE[status] || HYPOTHESIS_LIFECYCLE.GENERATED;
}

function buildHypothesisLifecycleRecord(partial = {}, opts = {}) {
  const lifecycle =
    opts.lifecycle ||
    partial.lifecycle ||
    mapSearchStatusToLifecycle(partial.status);

  return {
    id: partial.id || `hyp-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    text: partial.text || '',
    kind: partial.kind || 'terminology_search',
    searchTerms: Array.isArray(partial.searchTerms) ? partial.searchTerms.slice() : [],
    lifecycle,
    status: partial.status || lifecycle,
    confidence: partial.confidence != null ? partial.confidence : null,
    evidence: partial.evidence || null,
    rationale: partial.rationale || '',
    parentId: partial.parentId || null,
    spawnedFrom: partial.spawnedFrom || null,
    archivedAt: lifecycle === HYPOTHESIS_LIFECYCLE.ARCHIVED ? new Date().toISOString() : partial.archivedAt || null,
    archiveReason:
      partial.archiveReason ||
      (lifecycle === HYPOTHESIS_LIFECYCLE.REJECTED
        ? partial.rejectionReason || 'Hypothesis failed testing'
        : lifecycle === HYPOTHESIS_LIFECYCLE.ARCHIVED
          ? partial.archiveReason || 'Hypothesis inconclusive — archived for future reference'
          : null),
    lifecycleHistory: Array.isArray(partial.lifecycleHistory)
      ? partial.lifecycleHistory.slice()
      : [{ at: new Date().toISOString(), lifecycle, reason: opts.reason || 'Initial hypothesis' }],
  };
}

function transitionHypothesisLifecycle(hypothesis, nextLifecycle, reason = '') {
  const record = buildHypothesisLifecycleRecord(hypothesis);
  if (record.lifecycle === nextLifecycle) return record;

  const history = [
    ...record.lifecycleHistory,
    { at: new Date().toISOString(), lifecycle: nextLifecycle, reason: reason || `Transitioned to ${nextLifecycle}` },
  ];

  return buildHypothesisLifecycleRecord(
    {
      ...record,
      lifecycle: nextLifecycle,
      status: nextLifecycle,
      lifecycleHistory: history,
      archivedAt: nextLifecycle === HYPOTHESIS_LIFECYCLE.ARCHIVED ? new Date().toISOString() : record.archivedAt,
      archiveReason:
        nextLifecycle === HYPOTHESIS_LIFECYCLE.REJECTED || nextLifecycle === HYPOTHESIS_LIFECYCLE.ARCHIVED
          ? reason || record.archiveReason
          : record.archiveReason,
    },
    { lifecycle: nextLifecycle, reason }
  );
}

function markHypothesisTesting(hypothesis, reason = 'Investigation branch started') {
  return transitionHypothesisLifecycle(hypothesis, HYPOTHESIS_LIFECYCLE.TESTING, reason);
}

function markHypothesisSupported(hypothesis, reason = 'Evidence supports hypothesis') {
  return transitionHypothesisLifecycle(hypothesis, HYPOTHESIS_LIFECYCLE.SUPPORTED, reason);
}

function markHypothesisRejected(hypothesis, reason = 'Hypothesis failed testing') {
  return transitionHypothesisLifecycle(hypothesis, HYPOTHESIS_LIFECYCLE.REJECTED, reason);
}

function archiveHypothesis(hypothesis, reason = 'Hypothesis archived') {
  return transitionHypothesisLifecycle(hypothesis, HYPOTHESIS_LIFECYCLE.ARCHIVED, reason);
}

function applySearchHypothesisEvaluation(hypothesis, evaluation = {}) {
  const lifecycle = mapSearchStatusToLifecycle(evaluation.status || hypothesis.status);
  let record = buildHypothesisLifecycleRecord(hypothesis, { lifecycle });

  if (lifecycle === HYPOTHESIS_LIFECYCLE.SUPPORTED) {
    record = markHypothesisSupported(record, evaluation.reason || `${hypothesis.text} confirmed by evidence`);
  } else if (lifecycle === HYPOTHESIS_LIFECYCLE.REJECTED) {
    record = markHypothesisRejected(
      record,
      evaluation.reason || `${hypothesis.text} rejected — insufficient evidence (${evaluation.resultCount || 0} results)`
    );
  } else if (lifecycle === HYPOTHESIS_LIFECYCLE.ARCHIVED) {
    record = archiveHypothesis(record, evaluation.reason || 'Inconclusive — archived');
  } else if (lifecycle === HYPOTHESIS_LIFECYCLE.TESTING) {
    record = markHypothesisTesting(record, evaluation.reason || 'Testing in progress');
  }

  return {
    ...record,
    confidence: evaluation.confidence != null ? evaluation.confidence : record.confidence,
    evidence: evaluation.evidence || record.evidence,
  };
}

function generateReplacementHypotheses(rejectedHypotheses = [], marketDefinition = {}, opts = {}) {
  const replacements = [];
  const seen = new Set();

  for (const rejected of rejectedHypotheses) {
    if (rejected.lifecycle !== HYPOTHESIS_LIFECYCLE.REJECTED) continue;
    for (const adjacent of marketDefinition.adjacentMarkets || []) {
      const key = `adjacent:${adjacent}`;
      if (seen.has(key)) continue;
      seen.add(key);
      replacements.push(
        buildHypothesisLifecycleRecord(
          {
            text: `Market may self-describe using adjacent terminology: ${adjacent}`,
            searchTerms: [adjacent],
            rationale: `Replacement after rejecting: ${rejected.text}`,
            spawnedFrom: rejected.id,
            parentId: rejected.id,
          },
          { lifecycle: HYPOTHESIS_LIFECYCLE.GENERATED, reason: `Spawned after rejection of ${rejected.id}` }
        )
      );
    }
  }

  if (!replacements.length && opts.generateFollowUp) {
    replacements.push(
      buildHypothesisLifecycleRecord(
        {
          text: 'Market uses platform-native terminology rather than industry labels.',
          searchTerms: [],
          rationale: 'Fallback hypothesis when all terminology branches rejected.',
        },
        { lifecycle: HYPOTHESIS_LIFECYCLE.GENERATED, reason: 'Fallback after all hypotheses rejected' }
      )
    );
  }

  return replacements.slice(0, opts.maxReplacements || 3);
}

function summarizeHypothesisHistory(state = {}) {
  const all = [
    ...(state.activeHypotheses || []),
    ...(state.rejectedHypotheses || []),
    ...(state.archivedHypotheses || []),
  ];
  return all.map((h) => ({
    id: h.id,
    text: h.text,
    lifecycle: h.lifecycle,
    confidence: h.confidence,
    archiveReason: h.archiveReason || null,
    lifecycleHistory: h.lifecycleHistory || [],
  }));
}

module.exports = {
  HYPOTHESIS_LIFECYCLE,
  SEARCH_STATUS_TO_LIFECYCLE,
  mapSearchStatusToLifecycle,
  buildHypothesisLifecycleRecord,
  transitionHypothesisLifecycle,
  markHypothesisTesting,
  markHypothesisSupported,
  markHypothesisRejected,
  archiveHypothesis,
  applySearchHypothesisEvaluation,
  generateReplacementHypotheses,
  summarizeHypothesisHistory,
};
