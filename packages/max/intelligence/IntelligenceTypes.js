'use strict';

const { deepFreeze } = require('../reasoning/ReasoningTypes');

/** @typedef {'deck'|'recommendation'|'company'|'evidence'|'interaction'} NavKind */

/**
 * @typedef {object} NavRef
 * @property {string} type - company | recommendation | evidence | interaction | claim
 * @property {string} id
 * @property {string} label
 */

/**
 * @typedef {object} TrailStep
 * @property {NavKind|string} kind
 * @property {string|null} id
 * @property {string} label
 */

const NAV_TYPES = Object.freeze({
  COMPANY: 'company',
  RECOMMENDATION: 'recommendation',
  EVIDENCE: 'evidence',
  INTERACTION: 'interaction',
  CLAIM: 'claim',
});

const TRAIL_KINDS = Object.freeze({
  DECK: 'deck',
  RECOMMENDATION: 'recommendation',
  COMPANY: 'company',
  EVIDENCE: 'evidence',
  INTERACTION: 'interaction',
});

/**
 * @param {object} input
 * @returns {NavRef|null}
 */
function buildNavRef(input) {
  if (!input || input.id == null || input.id === '') return null;
  const type = String(input.type || NAV_TYPES.EVIDENCE);
  const id = String(input.id);
  const label =
    input.label != null && String(input.label).trim()
      ? String(input.label).trim()
      : id;
  return { type, id, label };
}

/**
 * Parse `rec:{tenantId}:{companyId}` — companyId may contain colons.
 * @param {string} recommendationId
 * @returns {{ tenantId: string, companyId: string }|null}
 */
function parseRecommendationId(recommendationId) {
  const raw = String(recommendationId || '');
  if (!raw.startsWith('rec:')) return null;
  const rest = raw.slice(4);
  const idx = rest.indexOf(':');
  if (idx <= 0 || idx === rest.length - 1) return null;
  return {
    tenantId: rest.slice(0, idx),
    companyId: rest.slice(idx + 1),
  };
}

/**
 * @param {string} tenantId
 * @param {string} companyId
 */
function buildRecommendationId(tenantId, companyId) {
  return `rec:${tenantId}:${companyId}`;
}

/**
 * Push a trail step (immutable).
 * @param {TrailStep[]} trail
 * @param {TrailStep} step
 */
function pushTrail(trail, step) {
  const next = Array.isArray(trail) ? trail.slice() : [];
  const kind = String((step && step.kind) || TRAIL_KINDS.DECK);
  const id = step && step.id != null ? String(step.id) : null;
  const label =
    step && step.label != null && String(step.label).trim()
      ? String(step.label).trim()
      : kind;
  const last = next[next.length - 1];
  if (
    last &&
    last.kind === kind &&
    String(last.id || '') === String(id || '')
  ) {
    return next;
  }
  next.push({ kind, id, label });
  return next;
}

/**
 * Pop to index (inclusive keep).
 * @param {TrailStep[]} trail
 * @param {number} index
 */
function popTrailTo(trail, index) {
  const list = Array.isArray(trail) ? trail : [];
  if (!Number.isFinite(index) || index < 0) return [];
  return list.slice(0, index + 1);
}

/**
 * MaxContext focus fields from trail tip.
 * @param {TrailStep[]} trail
 * @param {object} [base]
 */
function focusFromTrail(trail, base = {}) {
  const list = Array.isArray(trail) ? trail : [];
  const tip = list[list.length - 1] || null;
  let page = 'command-deck';
  let companyId = base.companyId || null;
  let recommendationId = base.recommendationId || null;
  let selectedEntity = null;

  if (!tip || tip.kind === TRAIL_KINDS.DECK) {
    page = 'command-deck';
  } else if (tip.kind === TRAIL_KINDS.RECOMMENDATION) {
    page = 'recommendation';
    recommendationId = tip.id;
    const parsed = parseRecommendationId(tip.id);
    if (parsed) companyId = parsed.companyId;
    selectedEntity = {
      id: String(tip.id || ''),
      type: 'recommendation',
      name: tip.label,
    };
  } else if (tip.kind === TRAIL_KINDS.COMPANY) {
    page = 'company';
    companyId = tip.id;
    selectedEntity = {
      id: String(tip.id || ''),
      type: 'company',
      name: tip.label,
    };
  } else if (
    tip.kind === TRAIL_KINDS.EVIDENCE ||
    tip.kind === TRAIL_KINDS.INTERACTION
  ) {
    page = 'timeline';
    selectedEntity = {
      id: String(tip.id || ''),
      type: tip.kind,
      name: tip.label,
    };
  }

  for (let i = list.length - 1; i >= 0; i -= 1) {
    const step = list[i];
    if (!companyId && step.kind === TRAIL_KINDS.COMPANY && step.id) {
      companyId = step.id;
    }
    if (
      !recommendationId &&
      step.kind === TRAIL_KINDS.RECOMMENDATION &&
      step.id
    ) {
      recommendationId = step.id;
      const parsed = parseRecommendationId(step.id);
      if (parsed && !companyId) companyId = parsed.companyId;
    }
  }

  if (tip && tip.kind === TRAIL_KINDS.EVIDENCE) {
    if (recommendationId) page = 'recommendation';
    else if (companyId) page = 'company';
  }

  return deepFreeze({
    page,
    companyId: companyId ? String(companyId) : null,
    recommendationId: recommendationId ? String(recommendationId) : null,
    selectedEntity,
    trail: list.map((s) => ({ ...s })),
  });
}

module.exports = {
  NAV_TYPES,
  TRAIL_KINDS,
  buildNavRef,
  parseRecommendationId,
  buildRecommendationId,
  pushTrail,
  popTrailTo,
  focusFromTrail,
};
