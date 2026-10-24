'use strict';

/**
 * SPEC-146 — Evidence Conflict Report builder.
 * Surfaces disagreement transparently in mission intelligence reports.
 */

const { subjectLabel } = require('./ConflictDetection');
const { buildConflictResolutionResult } = require('./types');

function formatConflictEntry(conflict) {
  const label = subjectLabel(conflict.subject);
  const claims = (conflict.conflictingClaims || []).map((c) => ({
    provider: c.sourceLabel || c.source,
    value: c.value,
    label: c.label,
    observedAt: c.observedAt || null,
  }));

  return {
    id: conflict.id,
    subject: conflict.subject,
    label,
    severity: conflict.severity,
    category: conflict.category,
    claims,
    resolution: {
      strategy: conflict.resolution?.strategy,
      workingEstimate: conflict.resolution?.workingEstimate,
      reason: conflict.resolution?.reason,
      resolved: conflict.resolution?.resolved === true,
    },
    confidence: conflict.confidence,
    unresolvedReason: conflict.unresolvedReason,
    recommendedProviders: conflict.recommendedProviders || [],
  };
}

/**
 * Build evidence conflicts section for mission intelligence report.
 * @param {object[]} conflicts — resolved EvidenceConflict objects
 * @param {object} [context]
 * @returns {object}
 */
function buildEvidenceConflictsSection(conflicts = [], context = {}) {
  const baseConfidence = context.baseConfidence != null ? context.baseConfidence : null;
  const resolved = conflicts.filter((c) => c.resolution?.resolved);
  const outstanding = conflicts.filter((c) => !c.resolution?.resolved);

  let adjustedConfidence = baseConfidence;
  if (baseConfidence != null) {
    let penalty = 0;
    for (const c of conflicts) {
      penalty += c.confidencePenalty || 0;
    }
    adjustedConfidence = Number(Math.max(0, baseConfidence - penalty).toFixed(2));
  }

  const entries = conflicts.map(formatConflictEntry);

  const summary = {
    detected: conflicts.length,
    resolved: resolved.length,
    outstanding: outstanding.length,
    recommendationConfidenceBefore: baseConfidence,
    recommendationConfidenceAfter: adjustedConfidence,
    confidenceReduced: baseConfidence != null && adjustedConfidence < baseConfidence,
  };

  return {
    version: 'SPEC-146',
    summary,
    conflicts: entries,
    investigationTasks: outstanding.map((c) => c.investigationTask).filter(Boolean),
    narrative: buildConflictNarrative(summary, entries),
  };
}

function buildConflictNarrative(summary, entries) {
  if (summary.detected === 0) {
    return 'No evidence conflicts detected.';
  }

  const parts = [
    `Evidence Conflicts: ${summary.detected} detected, ${summary.resolved} resolved, ${summary.outstanding} outstanding.`,
  ];

  if (summary.confidenceReduced && summary.recommendationConfidenceBefore != null) {
    parts.push(
      `Recommendation confidence reduced from ${summary.recommendationConfidenceBefore} to ${summary.recommendationConfidenceAfter}.`
    );
  }

  for (const entry of entries.slice(0, 3)) {
    if (entry.resolution.resolved && entry.resolution.reason) {
      parts.push(`${entry.label}: ${entry.resolution.reason}`);
    } else if (entry.unresolvedReason) {
      parts.push(`${entry.label}: ${entry.unresolvedReason}`);
    }
  }

  return parts.join(' ');
}

/**
 * Build full conflict resolution result for pipeline/investigation output.
 * @param {object} input
 * @returns {object}
 */
function buildConflictReport(input = {}) {
  const conflicts = Array.isArray(input.conflicts) ? input.conflicts : [];
  const entityId = input.entityId || null;
  const baseConfidence = input.baseConfidence;

  const section = buildEvidenceConflictsSection(conflicts, { baseConfidence });
  const investigationTasks = conflicts
    .filter((c) => !c.resolution?.resolved)
    .map((c) => c.investigationTask)
    .filter(Boolean);

  const recommendedProviders = [
    ...new Set(
      conflicts.flatMap((c) => c.recommendedProviders || [])
    ),
  ];

  return buildConflictResolutionResult({
    entityId,
    conflicts,
    confidenceAdjustment:
      baseConfidence != null && section.summary.recommendationConfidenceAfter != null
        ? Number((baseConfidence - section.summary.recommendationConfidenceAfter).toFixed(2))
        : 0,
    recommendedProviders,
    investigationTasks,
    report: section,
  });
}

module.exports = {
  formatConflictEntry,
  buildEvidenceConflictsSection,
  buildConflictReport,
  buildConflictNarrative,
};
