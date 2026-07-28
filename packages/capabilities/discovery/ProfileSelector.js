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
   * Delegates to SPEC-040 DiscoveryProfileResolver (deterministic precedence).
   *
   * @param {object} input
   * @param {string} input.objective
   * @param {string|number} [input.clientId]
   * @param {string|number} [input.tenantId]
   * @param {object} [input.constraints]
   * @returns {{ profile: object|null, selection: string, alternatives: object[], message: string, resolution: object }}
   */
  select(input = {}) {
    // Lazy require avoids circular load with DiscoveryProfileResolver helpers.
    const {
      createDiscoveryProfileResolver,
      PROFILE_SELECTION_REASONS,
    } = require('./DiscoveryProfileResolver');
    const resolver = createDiscoveryProfileResolver({ store: this._store });
    const resolution = resolver.resolve(input);

    if (resolution.blocked || !resolution.profile) {
      return {
        profile: null,
        selection: 'blocked',
        alternatives: resolution.alternatives || [],
        message: resolution.message || 'No Discovery Profile',
        resolution,
        blocked: true,
        blockingIssues: resolution.blockingIssues || [],
      };
    }

    return {
      profile: resolution.profile,
      selection: mapLegacySelection(
        resolution,
        PROFILE_SELECTION_REASONS
      ),
      alternatives: resolution.alternatives || [],
      message: resolution.message,
      resolution,
      blocked: false,
      blockingIssues: [],
    };
  }
}

/**
 * Map SPEC-040 selection reasons to legacy ProfileSelector selection strings.
 */
function mapLegacySelection(resolution, REASONS) {
  const selection = resolution.selection;
  switch (selection) {
    case REASONS.MISSION_CONSTRAINTS:
      return 'explicit';
    case REASONS.EXPLICIT_OVERRIDE:
      return 'pinned';
    case REASONS.PINNED_CLIENT:
      return (resolution.alternatives || []).length ? 'ambiguous' : 'matched';
    case REASONS.CLIENT_DEFAULT_GEOGRAPHY:
      return 'generated';
    case REASONS.MISSION_TYPE_DEFAULT: {
      const status =
        resolution.profile && resolution.profile.status;
      return status === 'temporary' ? 'generated' : 'matched';
    }
    case REASONS.BLOCKED:
      return 'blocked';
    default:
      return selection || 'matched';
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
