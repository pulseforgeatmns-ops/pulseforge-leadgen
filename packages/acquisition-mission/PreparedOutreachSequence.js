'use strict';

/**
 * SPEC-252 — Canonical prepared outreach sequence (cadence) normalization.
 * Legacy template catalogs are read only at PREPARE; OBSERVE never imports them.
 */

const { asText, nowIso } = require('./types');

const SOURCE_KINDS = Object.freeze({
  TEMPLATE_CATALOG: 'template_catalog',
  EXPLICIT: 'explicit',
  MISSION_POLICY: 'mission_policy',
  HISTORICAL_BACKFILL: 'historical_backfill',
});

const CADENCE_PROVENANCE = Object.freeze({
  APPROVAL_SNAPSHOT: 'approval_snapshot',
  HISTORICAL_ANNOTATION: 'historical_annotation',
  PAIGE_CONTRIBUTION: 'paige_contribution',
  IN_MEMORY_STORE: 'in_memory_store',
});

function defaultSequenceCatalog() {
  // eslint-disable-next-line global-require
  const { ANCHOR_DRAFT_SEQUENCES } = require('../../utils/anchorEmailTemplates');
  return ANCHOR_DRAFT_SEQUENCES;
}

function defaultClientSequenceMap() {
  // eslint-disable-next-line global-require
  const { CLIENT_SEQUENCE_MAP } = require('../../utils/sendingReadiness');
  return CLIENT_SEQUENCE_MAP;
}

function segmentToVerticalSlug(targetSegment) {
  const segment = asText(targetSegment).toLowerCase();
  if (!segment) return null;
  if (segment.includes('law')) return 'law_firm';
  if (segment.includes('account')) return 'accounting';
  return segment.replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '') || null;
}

function normalizeOutreachSequenceSteps(rawSteps = []) {
  if (!Array.isArray(rawSteps)) return [];
  const normalized = rawSteps
    .map((row, index) => {
      const day = Number(row?.day);
      if (!Number.isFinite(day)) return null;
      const stepNum = row?.step != null ? Number(row.step) : index;
      const step = {
        step: Number.isFinite(stepNum) ? stepNum : index,
        day,
        channel: asText(row?.channel) || 'email',
      };
      if (row?.variantId) step.variantId = asText(row.variantId);
      if (row?.candidateId) step.candidateId = asText(row.candidateId);
      return step;
    })
    .filter(Boolean)
    .sort((a, b) => a.day - b.day || a.step - b.step);

  const seenDays = new Set();
  for (const row of normalized) {
    if (seenDays.has(row.day)) {
      return [];
    }
    seenDays.add(row.day);
  }
  return normalized;
}

function extractOutreachSequenceSteps(payload = {}) {
  if (!payload || typeof payload !== 'object') return [];
  const raw = payload.outreachSequence;
  if (raw && typeof raw === 'object' && Array.isArray(raw.steps)) {
    return normalizeOutreachSequenceSteps(raw.steps);
  }
  if (Array.isArray(raw)) {
    return normalizeOutreachSequenceSteps(raw);
  }
  const legacyCandidates = [
    payload.steps,
    payload.sequence,
    payload.preparedSequence,
    payload.preparedOutreach?.steps,
    payload.prepared?.steps,
    payload.sequenceSteps,
  ];
  for (const candidate of legacyCandidates) {
    const steps = normalizeOutreachSequenceSteps(candidate);
    if (steps.length) return steps;
  }
  return [];
}

function outreachSequenceForRevisionHash(outreachSequence = null) {
  const steps = extractOutreachSequenceSteps({ outreachSequence });
  return steps.map(({ step, day, channel }) => ({
    step,
    day,
    channel: channel || 'email',
  }));
}

function freezeOutreachSequenceForApproval(outreachSequence = null) {
  const steps = extractOutreachSequenceSteps({ outreachSequence });
  if (!steps.length) return null;
  return {
    channel: asText(outreachSequence?.channel) || 'email',
    calendarDays: outreachSequence?.calendarDays !== false,
    steps,
  };
}

function collectCandidateVerticals(contributions = [], crmByProspectId = null) {
  const verticals = new Set();
  const maxRow = [...(contributions || [])]
    .reverse()
    .find((row) => row.specialist === 'max' && row.kind === 'prioritization');
  const maxPayload = maxRow?.payload || {};
  const ranked = maxPayload.rankedTargets || maxPayload.priorities || [];
  for (const target of ranked) {
    if (target?.vertical) verticals.add(asText(target.vertical).toLowerCase());
    const pid = target?.prospectId || target?.id;
    if (crmByProspectId && pid && crmByProspectId[pid]?.vertical) {
      verticals.add(asText(crmByProspectId[pid].vertical).toLowerCase());
    }
  }
  return [...verticals].filter(Boolean);
}

function resolveTemplateKey(clientId, vertical, clientSequenceMap = defaultClientSequenceMap()) {
  const clientKey = Number(clientId);
  if (!clientKey || !vertical) return null;
  const mapped = clientSequenceMap[clientKey]?.[vertical];
  return mapped || null;
}

function templateStepsFromCatalog(templateKey, catalog = defaultSequenceCatalog()) {
  const template = catalog?.[templateKey];
  if (!Array.isArray(template) || !template.length) return [];
  return normalizeOutreachSequenceSteps(
    template.map((row, index) => ({
      step: index,
      day: row.day,
      channel: 'email',
    }))
  );
}

/**
 * Resolve mission-wide outreach sequence at PREPARE.
 * Uses mission.targetSegment (homogeneous mission contract), not rankedTargets[0].
 * Returns null when catalog cannot resolve or candidate verticals conflict.
 */
function resolveOutreachSequenceAtPrepare(input = {}) {
  const {
    mission = {},
    contributions = [],
    clientId = mission.clientId ?? Number(mission.tenantId),
    catalog = defaultSequenceCatalog(),
    clientSequenceMap = defaultClientSequenceMap(),
    crmByProspectId = null,
    variants = [],
    now = new Date(),
  } = input;

  if (mission.outreachSequence) {
    const explicitSteps = extractOutreachSequenceSteps({ outreachSequence: mission.outreachSequence });
    if (explicitSteps.length) {
      return {
        channel: asText(mission.outreachSequence.channel) || 'email',
        calendarDays: mission.outreachSequence.calendarDays !== false,
        steps: linkStepZeroVariant(explicitSteps, variants),
        source: {
          kind: SOURCE_KINDS.EXPLICIT,
          normalizedAt: nowIso(now),
        },
      };
    }
  }

  const segmentVertical = segmentToVerticalSlug(mission.targetSegment);
  const candidateVerticals = collectCandidateVerticals(contributions, crmByProspectId);
  const verticalsToCheck = candidateVerticals.length
    ? candidateVerticals
    : (segmentVertical ? [segmentVertical] : []);

  if (!verticalsToCheck.length) return null;

  const templateKeys = new Set(
    verticalsToCheck
      .map((vertical) => resolveTemplateKey(clientId, vertical, clientSequenceMap))
      .filter(Boolean)
  );

  if (templateKeys.size !== 1) return null;

  const templateKey = [...templateKeys][0];
  const steps = templateStepsFromCatalog(templateKey, catalog);
  if (!steps.length) return null;

  const primaryVertical = segmentVertical || verticalsToCheck[0];

  return {
    id: templateKey,
    channel: 'email',
    calendarDays: true,
    steps: linkStepZeroVariant(steps, variants),
    source: {
      kind: SOURCE_KINDS.TEMPLATE_CATALOG,
      templateKey,
      clientId: Number(clientId) || null,
      vertical: primaryVertical,
      normalizedAt: nowIso(now),
    },
  };
}

function linkStepZeroVariant(steps = [], variants = []) {
  if (!steps.length || !Array.isArray(variants) || !variants.length) return steps;
  const primary = variants[0];
  return steps.map((row, index) => {
    if (index !== 0 && row.step !== 0) return row;
    return {
      ...row,
      variantId: primary.variantId || null,
      candidateId: primary.candidateId || null,
    };
  });
}

function buildHistoricalCadenceAnnotation(input = {}) {
  const {
    missionId,
    tenantId,
    executionRecordId,
    executionApprovalContributionId,
    preparedArtifactRevision,
    prospectId,
    outreachSequence,
    templateKey,
    reason,
    now = new Date(),
  } = input;

  const steps = extractOutreachSequenceSteps({ outreachSequence });
  if (!missionId || !preparedArtifactRevision || !steps.length) {
    return null;
  }

  const backfilledAt = nowIso(now);
  const id = [
    'cadence_ann',
    asText(executionRecordId).slice(0, 24) || asText(preparedArtifactRevision).slice(0, 12),
  ].join('_');

  return {
    id,
    missionId,
    tenantId: tenantId != null ? String(tenantId) : null,
    executionRecordId: executionRecordId || null,
    executionApprovalContributionId: executionApprovalContributionId || null,
    preparedArtifactRevision,
    prospectId: prospectId != null ? String(prospectId) : null,
    outreachSequence: {
      channel: asText(outreachSequence?.channel) || 'email',
      calendarDays: outreachSequence?.calendarDays !== false,
      steps,
    },
    source: {
      kind: SOURCE_KINDS.HISTORICAL_BACKFILL,
      templateKey: templateKey || outreachSequence?.source?.templateKey || null,
      reason: reason || 'Cadence reconstructed post-execution from client template catalog.',
    },
    backfilledAt,
    createdAt: backfilledAt,
  };
}

module.exports = {
  SOURCE_KINDS,
  CADENCE_PROVENANCE,
  segmentToVerticalSlug,
  normalizeOutreachSequenceSteps,
  extractOutreachSequenceSteps,
  outreachSequenceForRevisionHash,
  freezeOutreachSequenceForApproval,
  resolveOutreachSequenceAtPrepare,
  buildHistoricalCadenceAnnotation,
  linkStepZeroVariant,
  resolveTemplateKey,
  templateStepsFromCatalog,
  collectCandidateVerticals,
};
