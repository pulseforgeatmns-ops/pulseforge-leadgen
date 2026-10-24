'use strict';

/**
 * Evidence Planning — MissionIntent → EvidencePlan (SPEC-056 / ADR-040).
 *
 * Answers: What information is required to answer the operator's question?
 * Not: Which capability should run the business workflow?
 *
 * Capability Planning consumes EvidencePlan after this stage.
 */

const { BUILTIN_IDS } = require('../capabilities/types');
const { INTENT_CATEGORIES } = require('./MissionIntent');
const {
  EVIDENCE_TYPES,
  buildEvidencePlan,
  summarizeEvidencePlan,
} = require('./EvidencePlan');

/**
 * Intent category → required evidence types (descriptive only).
 * @type {Record<string, string[]>}
 */
const INTENT_EVIDENCE_REQUIREMENTS = Object.freeze({
  [INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS]: Object.freeze([
    EVIDENCE_TYPES.DISCOVERY_EXECUTION,
    EVIDENCE_TYPES.DISCOVERY_TRACE,
    EVIDENCE_TYPES.DISCOVERY_DIAGNOSTICS,
    EVIDENCE_TYPES.MISSION_STATE,
  ]),
  [INTENT_CATEGORIES.DISCOVERY_INVESTIGATION]: Object.freeze([
    EVIDENCE_TYPES.DISCOVERY_EXECUTION,
    EVIDENCE_TYPES.PROVIDER_SELECTION,
    EVIDENCE_TYPES.CANDIDATE_COUNTS,
    EVIDENCE_TYPES.VERIFICATION_RESULTS,
    EVIDENCE_TYPES.EXCEPTIONS,
    EVIDENCE_TYPES.DISCOVERY_TRACE,
    EVIDENCE_TYPES.DISCOVERY_DIAGNOSTICS,
  ]),
  [INTENT_CATEGORIES.DIAGNOSTICS]: Object.freeze([
    EVIDENCE_TYPES.MISSION_STATE,
    EVIDENCE_TYPES.MISSION_DIAGNOSTICS,
    EVIDENCE_TYPES.CAPABILITY_EXECUTION,
    EVIDENCE_TYPES.CAPABILITY_FAILURE,
  ]),
  [INTENT_CATEGORIES.CAMPAIGN_REVIEW]: Object.freeze([
    EVIDENCE_TYPES.MISSION_STATE,
  ]),
  [INTENT_CATEGORIES.OUTCOME_INTELLIGENCE]: Object.freeze([
    EVIDENCE_TYPES.MISSION_STATE,
  ]),
});

/**
 * Evidence type → preferred diagnostic producer (capability + stage).
 * Producers are read-only diagnostic capabilities where possible.
 */
const EVIDENCE_PRODUCERS = Object.freeze({
  [EVIDENCE_TYPES.DISCOVERY_EXECUTION]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.DISCOVERY_TRACE]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.DISCOVERY_DIAGNOSTICS]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.PROVIDER_SELECTION]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.CANDIDATE_COUNTS]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.VERIFICATION_RESULTS]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.EXCEPTIONS]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.MISSION_STATE]: Object.freeze({
    // MissionState is ambient — available from planner context when present
    capabilityId: null,
    stageId: null,
    label: null,
    ambient: true,
  }),
  [EVIDENCE_TYPES.MISSION_DIAGNOSTICS]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.CAPABILITY_EXECUTION]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
  [EVIDENCE_TYPES.CAPABILITY_FAILURE]: Object.freeze({
    capabilityId: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    stageId: 'discovery_diagnostics',
    label: 'Discovery Diagnostics',
  }),
});

/**
 * Resolve required evidence for a MissionIntent.
 * @param {object} missionIntent
 * @returns {string[]}
 */
function requiredEvidenceForIntent(missionIntent) {
  if (!missionIntent || typeof missionIntent !== 'object') return [];
  if (
    Array.isArray(missionIntent.requiresEvidence) &&
    missionIntent.requiresEvidence.length
  ) {
    return [...missionIntent.requiresEvidence];
  }
  const category =
    missionIntent.intentCategory || missionIntent.matchedIntent;
  const mapped = INTENT_EVIDENCE_REQUIREMENTS[category];
  return mapped ? [...mapped] : [];
}

/**
 * Normalize available artifact catalog into evidence type names.
 * Accepts PascalCase evidence types, snake_case aliases, or { type } objects.
 * @param {unknown} catalog
 * @returns {string[]}
 */
function catalogToAvailableTypes(catalog) {
  const available = new Set();
  if (!catalog) return [];

  const add = (raw) => {
    const t = normalizeEvidenceType(raw);
    if (t) available.add(t);
  };

  if (Array.isArray(catalog)) {
    for (const item of catalog) {
      if (typeof item === 'string') add(item);
      else if (item && typeof item === 'object') {
        add(item.type || item.artifactType || item.evidenceType || item.name);
      }
    }
  } else if (catalog instanceof Set) {
    for (const item of catalog) add(item);
  } else if (typeof catalog === 'object') {
    if (Array.isArray(catalog.available)) {
      return catalogToAvailableTypes(catalog.available);
    }
    if (Array.isArray(catalog.types)) {
      return catalogToAvailableTypes(catalog.types);
    }
    for (const key of Object.keys(catalog)) {
      if (catalog[key]) add(key);
    }
  }

  return [...available];
}

/**
 * @param {unknown} raw
 * @returns {string|null}
 */
function normalizeEvidenceType(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  // Already PascalCase evidence type
  if (Object.values(EVIDENCE_TYPES).includes(s)) return s;
  // snake_case / kebab → PascalGuess against known types
  const compact = s.replace(/[-_\s]/g, '').toLowerCase();
  for (const t of Object.values(EVIDENCE_TYPES)) {
    if (t.replace(/[-_\s]/g, '').toLowerCase() === compact) return t;
  }
  // Accept unknown PascalCase as-is for forward compatibility
  if (/^[A-Z][A-Za-z0-9]+$/.test(s)) return s;
  return null;
}

/**
 * Plan evidence acquisition for a MissionIntent.
 * @param {object} missionIntent
 * @param {object} [opts]
 * @param {unknown} [opts.availableArtifacts]
 * @param {object} [opts.registry] CapabilityRegistry
 * @param {string} [opts.now]
 * @returns {object} evidence_plan
 */
function planEvidence(missionIntent, opts = {}) {
  const required = requiredEvidenceForIntent(missionIntent);
  const available = catalogToAvailableTypes(opts.availableArtifacts);

  // Ambient MissionState: treat as available when any catalog is provided
  // OR when planner runs inside a mission context (default true for diagnostics).
  if (
    required.includes(EVIDENCE_TYPES.MISSION_STATE) &&
    !available.includes(EVIDENCE_TYPES.MISSION_STATE)
  ) {
    if (opts.missionStateAvailable !== false) {
      available.push(EVIDENCE_TYPES.MISSION_STATE);
    }
  }

  const missing = required.filter((t) => !available.includes(t));
  const acquisitions = [];
  const blocked = [];
  const scheduledCapabilityIds = new Set();

  for (const evidenceType of missing) {
    const producer = resolveProducer(evidenceType, opts.registry);
    if (!producer || !producer.capabilityId) {
      const ambient =
        EVIDENCE_PRODUCERS[evidenceType] &&
        EVIDENCE_PRODUCERS[evidenceType].ambient;
      if (ambient) {
        blocked.push({
          evidenceType,
          reason: 'Ambient evidence unavailable and no producer registered',
        });
      } else {
        blocked.push({
          evidenceType,
          reason: 'No registered producer',
        });
      }
      continue;
    }

    if (opts.registry) {
      const cap = opts.registry.get(producer.capabilityId);
      if (!cap) {
        blocked.push({
          evidenceType,
          reason: 'No registered producer',
          capabilityId: producer.capabilityId,
        });
        continue;
      }
      if (cap.enabled === false) {
        blocked.push({
          evidenceType,
          reason: `Producer disabled: ${cap.name || producer.capabilityId}`,
          capabilityId: producer.capabilityId,
        });
        continue;
      }
    }

    acquisitions.push({
      evidenceType,
      capabilityId: producer.capabilityId,
      stageId: producer.stageId,
      label: producer.label,
      strategy: 'diagnostic_capability',
    });
    scheduledCapabilityIds.add(producer.capabilityId);
  }

  const unableToAnswer = blocked.length > 0;

  return buildEvidencePlan({
    required,
    available,
    missing,
    acquired: [],
    acquisitions,
    blocked,
    unableToAnswer,
    reason: unableToAnswer ? formatBlockedReason(blocked) : null,
    intentCategory:
      (missionIntent &&
        (missionIntent.intentCategory || missionIntent.matchedIntent)) ||
      null,
    goal: (missionIntent && missionIntent.goal) || null,
    createdAt: opts.now || new Date().toISOString(),
  });
}

/**
 * Unique diagnostic execution stages implied by an EvidencePlan.
 * @param {object} evidencePlan
 * @returns {{ stageId: string, capabilityId: string, label: string }[]}
 */
function acquisitionStages(evidencePlan) {
  if (!evidencePlan || !Array.isArray(evidencePlan.acquisitions)) return [];
  const byStage = new Map();
  for (const a of evidencePlan.acquisitions) {
    if (!a.stageId || !a.capabilityId) continue;
    if (byStage.has(a.stageId)) continue;
    byStage.set(a.stageId, {
      stageId: a.stageId,
      capabilityId: a.capabilityId,
      label: a.label || a.capabilityId,
    });
  }
  return [...byStage.values()];
}

function resolveProducer(evidenceType, registry) {
  const mapped = EVIDENCE_PRODUCERS[evidenceType];
  if (mapped) return mapped;

  // Fallback: ask registry for producers of this artifact type / alias
  if (registry && typeof registry.producersOf === 'function') {
    const producers = registry.producersOf(evidenceType) || [];
    const first = producers[0];
    if (first && first.id) {
      return {
        capabilityId: first.id,
        stageId: first.id,
        label: first.name || first.id,
      };
    }
  }
  return null;
}

function formatBlockedReason(blocked) {
  const lines = (blocked || []).map(
    (b) => `${b.evidenceType}: ${b.reason || 'No registered producer'}`
  );
  return `Unable to answer. Missing evidence: ${lines
    .map((l) => l.split(':')[0])
    .join(', ')}. Reason: ${lines.map((l) => l.split(':').slice(1).join(':').trim()).join('; ') || 'No registered producer.'}`;
}

module.exports = {
  INTENT_EVIDENCE_REQUIREMENTS,
  EVIDENCE_PRODUCERS,
  requiredEvidenceForIntent,
  catalogToAvailableTypes,
  normalizeEvidenceType,
  planEvidence,
  acquisitionStages,
  summarizeEvidencePlan,
};
