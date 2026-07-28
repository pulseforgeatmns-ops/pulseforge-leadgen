'use strict';

/**
 * Deterministic Discovery Profile resolution (SPEC-040 / ADR-026).
 *
 * Precedence:
 *   Mission Constraints
 *        ↓
 *   Explicit Operator Override
 *        ↓
 *   Pinned Client Discovery Profile
 *        ↓
 *   Client Default Geography
 *        ↓
 *   Mission Type Default
 *
 * Never silently falls back to another geography when a client profile exists.
 */

const { buildDiscoveryProfile } = require('./types');
const { createDiscoveryProfileStore } = require('./DiscoveryProfileStore');
const {
  synthesizeTemporaryProfile,
  inferClientId,
  scoreProfileMatch,
} = require('./ProfileSelector');

const PROFILE_SELECTION_REASONS = Object.freeze({
  MISSION_CONSTRAINTS: 'mission_constraints',
  EXPLICIT_OVERRIDE: 'explicit_operator_override',
  PINNED_CLIENT: 'pinned_client_profile',
  CLIENT_DEFAULT_GEOGRAPHY: 'client_default_geography',
  MISSION_TYPE_DEFAULT: 'mission_type_default',
  BLOCKED: 'blocked',
});

const REASON_LABELS = Object.freeze({
  [PROFILE_SELECTION_REASONS.MISSION_CONSTRAINTS]: 'Mission constraints',
  [PROFILE_SELECTION_REASONS.EXPLICIT_OVERRIDE]: 'Explicit operator override',
  [PROFILE_SELECTION_REASONS.PINNED_CLIENT]: 'Pinned client profile',
  [PROFILE_SELECTION_REASONS.CLIENT_DEFAULT_GEOGRAPHY]: 'Client default geography',
  [PROFILE_SELECTION_REASONS.MISSION_TYPE_DEFAULT]: 'Mission type default',
  [PROFILE_SELECTION_REASONS.BLOCKED]: 'No Discovery Profile',
});

/**
 * @param {object} [deps]
 * @param {object} [deps.store]
 */
class DiscoveryProfileResolver {
  constructor(deps = {}) {
    this._store = deps.store || createDiscoveryProfileStore();
  }

  get store() {
    return this._store;
  }

  /**
   * @param {object} input
   * @param {string} [input.objective]
   * @param {string|number} [input.clientId]
   * @param {string|number} [input.tenantId]
   * @param {object} [input.constraints]
   * @param {string} [input.missionType]
   * @returns {object} Resolution Report
   */
  resolve(input = {}) {
    const constraints =
      input.constraints && typeof input.constraints === 'object'
        ? input.constraints
        : {};
    const objective = String(input.objective || '');
    const overridesApplied = [];

    const clientId =
      input.clientId != null
        ? input.clientId
        : inferClientId(objective) != null
          ? inferClientId(objective)
          : input.tenantId;

    // 1. Mission Constraints — full profile object already bound by planner
    if (
      constraints.discoveryProfile &&
      typeof constraints.discoveryProfile === 'object' &&
      constraints.discoveryProfileBound === true
    ) {
      const profile = buildDiscoveryProfile(constraints.discoveryProfile);
      if (profile.id && isGeographyValid(profile.geography)) {
        return buildReport({
          profile: snapshotProfile(this._store, profile),
          selection: PROFILE_SELECTION_REASONS.MISSION_CONSTRAINTS,
          confidence: 1.0,
          overridesApplied,
          alternatives: [],
          message: `Using Discovery Profile: ${profile.name}.`,
        });
      }
    }

    // 2. Explicit Operator Override
    const overrideId =
      constraints.operatorOverrideProfileId ||
      (constraints.discoveryProfileOverride &&
        constraints.discoveryProfileOverride.id) ||
      null;
    if (overrideId) {
      const profile = this._store.get(
        overrideId,
        constraints.discoveryProfileVersion ||
          (constraints.discoveryProfileOverride &&
            constraints.discoveryProfileOverride.version)
      );
      if (profile) {
        overridesApplied.push({
          type: 'profile_id',
          value: String(overrideId),
        });
        return buildReport({
          profile: snapshotProfile(this._store, profile),
          selection: PROFILE_SELECTION_REASONS.EXPLICIT_OVERRIDE,
          confidence: 1.0,
          overridesApplied,
          alternatives: [],
          message: `Using Discovery Profile: ${profile.name} (operator override).`,
        });
      }
      return blockedReport({
        message: `Operator override profile not found: ${overrideId}`,
        blockingIssues: [
          `No Discovery Profile for override id ${overrideId}`,
        ],
        overridesApplied,
      });
    }

    if (constraints.discoveryProfileId) {
      const profile = this._store.get(
        constraints.discoveryProfileId,
        constraints.discoveryProfileVersion
      );
      if (profile) {
        overridesApplied.push({
          type: 'discoveryProfileId',
          value: String(constraints.discoveryProfileId),
        });
        return buildReport({
          profile: snapshotProfile(this._store, profile),
          selection: PROFILE_SELECTION_REASONS.EXPLICIT_OVERRIDE,
          confidence: 1.0,
          overridesApplied,
          alternatives: [],
          message: `Using Discovery Profile: ${profile.name}.`,
        });
      }
      return blockedReport({
        message: `Pinned Discovery Profile not found: ${constraints.discoveryProfileId}`,
        blockingIssues: [
          `No Discovery Profile for id ${constraints.discoveryProfileId}`,
        ],
        overridesApplied,
      });
    }

    if (
      constraints.discoveryProfile &&
      typeof constraints.discoveryProfile === 'object' &&
      constraints.discoveryProfile.id
    ) {
      const profile = buildDiscoveryProfile(constraints.discoveryProfile);
      if (!isGeographyValid(profile.geography) && profile.status !== 'temporary') {
        return blockedReport({
          message: 'Discovery Profile geography is invalid',
          blockingIssues: ['Discovery Profile geography is invalid'],
          overridesApplied,
        });
      }
      overridesApplied.push({ type: 'profile_object', value: profile.id });
      return buildReport({
        profile,
        selection: PROFILE_SELECTION_REASONS.EXPLICIT_OVERRIDE,
        confidence: 1.0,
        overridesApplied,
        alternatives: [],
        message: `Using Discovery Profile: ${profile.name}.`,
      });
    }

    // 3. Pinned Client Discovery Profile — ONLY client-scoped profiles
    const clientProfiles = listClientProfiles(this._store, clientId, input.tenantId);
    if (clientProfiles.length > 0) {
      const pinned = pickClientProfile(clientProfiles, objective, constraints);
      return buildReport({
        profile: snapshotProfile(this._store, pinned.profile),
        selection: PROFILE_SELECTION_REASONS.PINNED_CLIENT,
        confidence: pinned.confidence,
        overridesApplied,
        alternatives: pinned.alternatives,
        message:
          pinned.confidence < 1
            ? `Using Discovery Profile: ${pinned.profile.name}. Other client profiles available — confirm if needed.`
            : `Using Discovery Profile: ${pinned.profile.name}.`,
      });
    }

    // 4. Client Default Geography — synthesize within known client geography
    const clientGeo =
      constraints.clientGeography || constraints.defaultGeography || null;
    if (clientGeo && (clientGeo.label || (clientGeo.cities && clientGeo.cities.length))) {
      const temp = synthesizeTemporaryProfile({
        objective,
        clientId,
        constraints: { ...constraints },
      });
      const withGeo = buildDiscoveryProfile({
        ...temp,
        name: `Temporary — ${clientGeo.label || 'Client geography'}`,
        geography: normalizeGeo(clientGeo),
        status: 'temporary',
      });
      return buildReport({
        profile: withGeo,
        selection: PROFILE_SELECTION_REASONS.CLIENT_DEFAULT_GEOGRAPHY,
        confidence: 0.7,
        overridesApplied,
        alternatives: [],
        message: `Using Discovery Profile: ${withGeo.name} (client default geography).`,
      });
    }

    // 5. Mission Type Default — library match or temporary (no client profiles)
    const globalCandidates = this._store.list({ status: 'active' }) || [];
    const scored = globalCandidates
      .map((p) => ({
        profile: p,
        score: scoreProfileMatch(p, objective, clientId),
      }))
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score);

    if (scored.length > 0) {
      const best = scored[0];
      const alternatives = scored.slice(1, 4).map((s) => s.profile);
      const needsChoice =
        scored.length > 1 && scored[1].score >= best.score * 0.9;
      return buildReport({
        profile: snapshotProfile(this._store, best.profile),
        selection: PROFILE_SELECTION_REASONS.MISSION_TYPE_DEFAULT,
        confidence: needsChoice ? 0.75 : 0.85,
        overridesApplied,
        alternatives,
        message: needsChoice
          ? `Using Discovery Profile: ${best.profile.name}. Other matches available — confirm if needed.`
          : `Using Discovery Profile: ${best.profile.name}.`,
      });
    }

    const temp = synthesizeTemporaryProfile({
      objective,
      clientId,
      constraints,
    });
    return buildReport({
      profile: temp,
      selection: PROFILE_SELECTION_REASONS.MISSION_TYPE_DEFAULT,
      confidence: 0.55,
      overridesApplied,
      alternatives: [],
      message: `Using Discovery Profile: ${temp.name} (temporary for this mission).`,
    });
  }
}

function snapshotProfile(store, profile) {
  if (store && typeof store.snapshot === 'function') {
    return store.snapshot(profile);
  }
  return buildDiscoveryProfile(profile);
}

/**
 * @param {object} store
 * @param {string|number} clientId
 * @param {string|number} [tenantId]
 */
function listClientProfiles(store, clientId, tenantId) {
  if (clientId == null && tenantId == null) return [];
  const all = store.list({ status: 'active' }) || [];
  return all.filter((p) => {
    if (
      clientId != null &&
      Array.isArray(p.clientIds) &&
      p.clientIds.some((c) => String(c) === String(clientId))
    ) {
      return true;
    }
    if (
      tenantId != null &&
      p.tenantId != null &&
      String(p.tenantId) === String(tenantId)
    ) {
      return true;
    }
    return false;
  });
}

/**
 * Pick among client profiles only — never another geography outside the set.
 */
function pickClientProfile(clientProfiles, objective, constraints) {
  if (clientProfiles.length === 1) {
    return {
      profile: clientProfiles[0],
      confidence: 1.0,
      alternatives: [],
    };
  }

  const defaults = clientProfiles.filter(
    (p) => p.isDefault === true || p.clientDefault === true
  );
  if (defaults.length === 1) {
    return {
      profile: defaults[0],
      confidence: 1.0,
      alternatives: clientProfiles.filter((p) => p.id !== defaults[0].id),
    };
  }

  const geoHint =
    (constraints.geography && String(constraints.geography).toLowerCase()) ||
    extractGeoToken(objective);

  if (geoHint) {
    const geoMatched = clientProfiles
      .map((p) => ({
        profile: p,
        score: geoScore(p, geoHint) + scoreProfileMatch(p, objective, null),
      }))
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score);

    if (geoMatched.length === 1) {
      return {
        profile: geoMatched[0].profile,
        confidence: 1.0,
        alternatives: clientProfiles.filter(
          (p) => p.id !== geoMatched[0].profile.id
        ),
      };
    }
    if (
      geoMatched.length > 1 &&
      geoMatched[0].score > geoMatched[1].score * 1.15
    ) {
      return {
        profile: geoMatched[0].profile,
        confidence: 0.9,
        alternatives: geoMatched.slice(1).map((g) => g.profile),
      };
    }
  }

  const sorted = [...clientProfiles].sort((a, b) =>
    String(a.id).localeCompare(String(b.id))
  );
  return {
    profile: sorted[0],
    confidence: 0.8,
    alternatives: sorted.slice(1),
  };
}

function extractGeoToken(objective) {
  const lower = String(objective || '').toLowerCase();
  for (const city of [
    'manchester',
    'providence',
    'boston',
    'nashville',
    'charleston',
  ]) {
    if (lower.includes(city)) return city;
  }
  return null;
}

function geoScore(profile, geoHint) {
  const label = String(profile.geography?.label || '').toLowerCase();
  const cities = (profile.geography?.cities || []).map((c) =>
    String(c).toLowerCase()
  );
  const name = String(profile.name || '').toLowerCase();
  let score = 0;
  if (label.includes(geoHint)) score += 40;
  if (cities.some((c) => c.includes(geoHint))) score += 30;
  if (name.includes(geoHint)) score += 20;
  return score;
}

function isGeographyValid(geo) {
  if (!geo) return false;
  if (geo.label && String(geo.label).trim()) return true;
  if (Array.isArray(geo.cities) && geo.cities.length > 0) return true;
  return false;
}

function normalizeGeo(geo) {
  if (!geo) return { label: '', cities: [], state: null, radiusMiles: null };
  if (typeof geo === 'string') {
    return { label: geo, cities: [], state: null, radiusMiles: null };
  }
  return {
    label: String(geo.label || ''),
    cities: Array.isArray(geo.cities) ? geo.cities.map(String) : [],
    state: geo.state != null ? String(geo.state) : null,
    radiusMiles:
      geo.radiusMiles != null && Number.isFinite(Number(geo.radiusMiles))
        ? Number(geo.radiusMiles)
        : null,
  };
}

function buildReport(input) {
  const profile = input.profile;
  const selection = input.selection;
  return {
    profile,
    selection,
    reason: REASON_LABELS[selection] || selection,
    geography: profile.geography || normalizeGeo(null),
    confidence: Number(input.confidence) || 0,
    overridesApplied: Array.isArray(input.overridesApplied)
      ? input.overridesApplied
      : [],
    alternatives: Array.isArray(input.alternatives) ? input.alternatives : [],
    message: input.message || `Using Discovery Profile: ${profile.name}.`,
    blocked: false,
    blockingIssues: [],
  };
}

function blockedReport(input) {
  return {
    profile: null,
    selection: PROFILE_SELECTION_REASONS.BLOCKED,
    reason: REASON_LABELS[PROFILE_SELECTION_REASONS.BLOCKED],
    geography: normalizeGeo(null),
    confidence: 0,
    overridesApplied: input.overridesApplied || [],
    alternatives: input.alternatives || [],
    message: input.message || 'No Discovery Profile',
    blocked: true,
    blockingIssues: input.blockingIssues || ['No Discovery Profile'],
  };
}

function createDiscoveryProfileResolver(deps) {
  return new DiscoveryProfileResolver(deps);
}

module.exports = {
  DiscoveryProfileResolver,
  createDiscoveryProfileResolver,
  PROFILE_SELECTION_REASONS,
  REASON_LABELS,
  listClientProfiles,
  isGeographyValid,
};
