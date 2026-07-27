'use strict';

/**
 * Mission Planner → Discovery Profile selection (SPEC-024).
 */

const { buildDiscoveryProfile } = require('./types');
const { createDiscoveryProfileStore } = require('./DiscoveryProfileStore');

const CLIENT_NAME_HINTS = Object.freeze({
  'anchor cleaning': 10,
  anchor: 10,
  mshi: 2,
  'mountain state': 2,
  'pulseforge nashville': 5,
  nashville: 5,
});

/**
 * @param {object} deps
 * @param {import('./DiscoveryProfileStore').DiscoveryProfileStore} deps.store
 */
class ProfileSelector {
  constructor(deps = {}) {
    this._store = deps.store || createDiscoveryProfileStore();
  }

  get store() {
    return this._store;
  }

  /**
   * Select or synthesize a Discovery Profile for a mission objective.
   *
   * @param {object} input
   * @param {string} input.objective
   * @param {string|number} [input.clientId]
   * @param {string|number} [input.tenantId]
   * @param {object} [input.constraints]
   * @returns {{ profile: object, selection: string, alternatives: object[], message: string }}
   */
  select(input = {}) {
    const constraints =
      input.constraints && typeof input.constraints === 'object'
        ? input.constraints
        : {};
    const objective = String(input.objective || '');

    // Explicit pin
    if (constraints.discoveryProfile && typeof constraints.discoveryProfile === 'object') {
      const profile = buildDiscoveryProfile(constraints.discoveryProfile);
      return {
        profile,
        selection: 'explicit',
        alternatives: [],
        message: `Using Discovery Profile: ${profile.name}.`,
      };
    }

    if (constraints.discoveryProfileId) {
      const profile = this._store.get(
        constraints.discoveryProfileId,
        constraints.discoveryProfileVersion
      );
      if (profile) {
        return {
          profile: this._store.snapshot(profile),
          selection: 'pinned',
          alternatives: [],
          message: `Using Discovery Profile: ${profile.name}.`,
        };
      }
    }

    const clientId =
      input.clientId != null
        ? input.clientId
        : inferClientId(objective) != null
          ? inferClientId(objective)
          : input.tenantId;

    const candidates = this._store.list({
      clientId,
      tenantId: input.tenantId,
      status: 'active',
    });

    const scored = candidates
      .map((p) => ({
        profile: p,
        score: scoreProfileMatch(p, objective, clientId),
      }))
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score);

    if (scored.length === 0) {
      const temp = synthesizeTemporaryProfile({
        objective,
        clientId,
        constraints,
      });
      return {
        profile: temp,
        selection: 'generated',
        alternatives: [],
        message: `Using Discovery Profile: ${temp.name} (temporary for this mission).`,
      };
    }

    // Multiple strong matches → operator should choose; pick best for now + list alts
    const best = scored[0];
    const alternatives = scored.slice(1, 4).map((s) => s.profile);
    const needsChoice =
      scored.length > 1 && scored[1].score >= best.score * 0.9;

    return {
      profile: this._store.snapshot(best.profile),
      selection: needsChoice ? 'ambiguous' : 'matched',
      alternatives,
      message: needsChoice
        ? `Using Discovery Profile: ${best.profile.name}. Other matches available — confirm if needed.`
        : `Using Discovery Profile: ${best.profile.name}.`,
    };
  }
}

/**
 * @param {string} objective
 * @returns {number|null}
 */
function inferClientId(objective) {
  const lower = String(objective || '').toLowerCase();
  for (const [hint, id] of Object.entries(CLIENT_NAME_HINTS)) {
    if (lower.includes(hint)) return id;
  }
  return null;
}

/**
 * @param {object} profile
 * @param {string} objective
 * @param {string|number} clientId
 */
function scoreProfileMatch(profile, objective, clientId) {
  let score = 0;
  const lower = String(objective || '').toLowerCase();
  const name = String(profile.name || '').toLowerCase();
  const desc = String(profile.description || '').toLowerCase();

  if (
    clientId != null &&
    Array.isArray(profile.clientIds) &&
    profile.clientIds.some((c) => String(c) === String(clientId))
  ) {
    score += 50;
  }

  if (lower.includes('anchor') && name.includes('manchester')) score += 30;
  if (lower.includes('manchester') && name.includes('manchester')) score += 25;
  if (lower.includes('providence') && name.includes('providence')) score += 25;
  if (lower.includes('boston') && name.includes('boston')) score += 25;

  if (lower.includes('overflow') && name.includes('overflow')) score += 40;
  if (lower.includes('acquisition') && name.includes('retiring')) score += 20;
  if (lower.includes('law') && name.includes('law')) score += 35;
  if (lower.includes('dental') && name.includes('dental')) score += 35;
  if (lower.includes('property') && name.includes('property')) score += 30;
  if (lower.includes('window') && name.includes('window')) score += 35;
  if (lower.includes('campaign') && name.includes('commercial cleaning')) score += 20;
  if (lower.includes('prospect') && name.includes('commercial cleaning')) score += 15;

  // Geography label tokens in objective
  const geoLabel = String(profile.geography?.label || '').toLowerCase();
  if (geoLabel && lower.includes(geoLabel.split(',')[0].trim())) score += 15;

  // Soft boost for commercial cleaning default when objective is generic campaign
  if (
    /campaign|discover|prospect/i.test(lower) &&
    name.includes('commercial cleaning') &&
    name.includes('manchester')
  ) {
    score += 10;
  }

  if (desc.includes('anchor') && lower.includes('anchor')) score += 10;

  return score;
}

/**
 * Generate a temporary (non-persisted) profile for novel missions.
 */
function synthesizeTemporaryProfile(input) {
  const objective = String(input.objective || 'Prospect Discovery');
  const geo = extractGeography(objective) || {
    label: 'Service area',
    cities: [],
    state: null,
    radiusMiles: null,
  };
  const industries = extractIndustries(objective);
  const targetCount =
    (input.constraints && Number(input.constraints.targetCount)) || 50;

  return buildDiscoveryProfile({
    id: `dp_temp_${Date.now().toString(36)}`,
    name: `Temporary — ${geo.label || 'Custom'}`,
    description: `Auto-generated for: ${objective.slice(0, 120)}`,
    tenantId: input.clientId != null ? String(input.clientId) : null,
    clientIds: input.clientId != null ? [Number(input.clientId) || input.clientId] : [],
    industryTargets: industries.length
      ? industries
      : ['Professional Offices', 'Commercial Property Management'],
    geography: geo,
    targetCount,
    requiredSignals: ['active_website', 'verified_address'],
    preferredSignals: ['commercial_office', 'professional_services'],
    excludedSignals: ['residential_only', 'closed_business', 'existing_prospect'],
    minimumConfidence: 0.7,
    version: '1.0-temp',
    status: 'temporary',
  });
}

function extractGeography(objective) {
  const m = /\bin\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?(?:,\s*[A-Z]{2})?)/.exec(
    objective
  );
  if (m) {
    const label = m[1].trim();
    const parts = label.split(',').map((s) => s.trim());
    return {
      label,
      cities: [parts[0]],
      state: parts[1] || null,
      radiusMiles: 20,
    };
  }
  if (/manchester/i.test(objective)) {
    return {
      label: 'Manchester, NH',
      cities: ['Manchester'],
      state: 'NH',
      radiusMiles: 20,
    };
  }
  return null;
}

function extractIndustries(objective) {
  const lower = objective.toLowerCase();
  const found = [];
  const map = [
    [/law\s*firm|attorney|legal/i, 'Law Firms'],
    [/cpa|accounting|accountant/i, 'CPA Firms'],
    [/medical|dental|doctor|clinic/i, 'Medical Offices'],
    [/property\s*manag/i, 'Commercial Property Management'],
    [/professional\s*service|office/i, 'Professional Offices'],
    [/cleaning\s*compan|janitorial/i, 'Commercial Cleaning'],
  ];
  for (const [re, label] of map) {
    if (re.test(lower)) found.push(label);
  }
  return found;
}

function createProfileSelector(deps) {
  return new ProfileSelector(deps);
}

module.exports = {
  ProfileSelector,
  createProfileSelector,
  inferClientId,
  scoreProfileMatch,
  synthesizeTemporaryProfile,
  CLIENT_NAME_HINTS,
};
