'use strict';

/**
 * CapabilityRegistry — discover and resolve capabilities (SPEC-023 / SPEC-054).
 * MissionPlanner never imports concrete implementations — only registry queries.
 */

class CapabilityRegistry {
  constructor() {
    /** @type {Map<string, object>} */
    this._byId = new Map();
  }

  /**
   * @param {object} capability
   */
  register(capability) {
    assertCapability(capability);
    const id = String(capability.id);
    if (this._byId.has(id)) {
      throw new Error(`Capability already registered: ${id}`);
    }
    this._byId.set(id, capability);
    return this;
  }

  /**
   * @param {string} id
   * @returns {object|null}
   */
  get(id) {
    return this._byId.get(String(id)) || null;
  }

  /**
   * @param {string} id
   * @returns {boolean}
   */
  isEnabled(id) {
    const cap = this.get(id);
    if (!cap) return false;
    return cap.enabled !== false;
  }

  /**
   * @returns {object[]}
   */
  list() {
    return [...this._byId.values()].sort((a, b) =>
      String(a.id).localeCompare(String(b.id))
    );
  }

  /**
   * Discover capabilities matching required outcome tags.
   * @param {string[]|object} query - outcome tags or { outcomeTags, category, enabledOnly }
   * @returns {object[]}
   */
  discover(query) {
    const tags = normalizeTags(query);
    const category =
      query && typeof query === 'object' && !Array.isArray(query)
        ? query.category || null
        : null;
    const enabledOnly =
      query && typeof query === 'object' && !Array.isArray(query)
        ? query.enabledOnly !== false
        : true;

    return this.list().filter((cap) => {
      if (enabledOnly && cap.enabled === false) return false;
      if (category && cap.category !== category) return false;
      if (!tags.length) return true;
      const outcomeTags = Array.isArray(cap.outcomeTags)
        ? cap.outcomeTags.map(String)
        : [];
      return tags.every((t) => outcomeTags.includes(t));
    });
  }

  /**
   * Who produces artifact X? (SPEC-054)
   * @param {string} artifactType - snake_case or PascalCase
   * @param {object} [opts]
   * @param {boolean} [opts.enabledOnly=true]
   * @param {number} [opts.version] - require matching version when set
   * @returns {object[]} ranked producers (lower acquisitionCost first)
   */
  producersOf(artifactType, opts = {}) {
    const needles = artifactNeedles(artifactType);
    if (!needles.length) return [];
    const enabledOnly = opts.enabledOnly !== false;
    const requiredVersion =
      opts.version != null ? Number(opts.version) : null;
    const out = [];
    for (const cap of this.list()) {
      if (enabledOnly && cap.enabled === false) continue;
      if (
        requiredVersion != null &&
        Number(cap.version) !== requiredVersion
      ) {
        continue;
      }
      const produces = [
        ...(Array.isArray(cap.produces) ? cap.produces : []),
      ].map(String);
      if (!produces.some((p) => needles.includes(normalizeArtifactKey(p)))) {
        continue;
      }
      out.push({
        ...cap,
        acquisitionCost: Number.isFinite(Number(cap.acquisitionCost))
          ? Number(cap.acquisitionCost)
          : DEFAULT_ACQUISITION_COST[cap.id] ?? 500,
      });
    }
    return out.sort((a, b) => a.acquisitionCost - b.acquisitionCost);
  }

  /**
   * Who consumes artifact X? (SPEC-054)
   * @param {string} artifactType
   * @param {object} [opts]
   * @param {boolean} [opts.enabledOnly=true]
   * @returns {object[]}
   */
  consumersOf(artifactType, opts = {}) {
    const needles = artifactNeedles(artifactType);
    if (!needles.length) return [];
    const enabledOnly = opts.enabledOnly !== false;
    return this.list().filter((cap) => {
      if (enabledOnly && cap.enabled === false) return false;
      const consumes = [
        ...(Array.isArray(cap.requires) ? cap.requires : []),
        ...(Array.isArray(cap.consumes) ? cap.consumes : []),
      ].map(String);
      return consumes.some((c) => needles.includes(normalizeArtifactKey(c)));
    });
  }

  /**
   * Resolve mission / operator text to a registered capability (SPEC-054).
   * @param {string} text
   * @param {object} [opts]
   * @param {boolean} [opts.enabledOnly=true]
   * @returns {{
   *   known: boolean,
   *   capabilityId: string|null,
   *   capability: object|null,
   *   confidence: number,
   *   matchKind: string|null,
   *   matchedAlias: string|null
   * }}
   */
  resolveAlias(text, opts = {}) {
    const raw = String(text || '').trim();
    if (!raw) {
      return emptyAliasMatch();
    }
    const enabledOnly = opts.enabledOnly !== false;
    const lower = raw.toLowerCase().replace(/\s+/g, ' ').trim();
    const underscored = lower.replace(/[\s-]+/g, '_');

    // Direct id
    const byId = this.get(underscored) || this.get(lower);
    if (byId && (!enabledOnly || byId.enabled !== false)) {
      return {
        known: true,
        capabilityId: byId.id,
        capability: byId,
        confidence: 1,
        matchKind: 'id',
        matchedAlias: byId.id,
      };
    }

    // Exact name / mission alias
    for (const cap of this.list()) {
      if (enabledOnly && cap.enabled === false) continue;
      const names = [
        cap.id,
        cap.name,
        ...(Array.isArray(cap.missionAliases) ? cap.missionAliases : []),
      ]
        .filter(Boolean)
        .map((s) => String(s).toLowerCase().replace(/\s+/g, ' ').trim());
      if (names.includes(lower) || names.includes(underscored)) {
        return {
          known: true,
          capabilityId: cap.id,
          capability: cap,
          confidence: 0.98,
          matchKind: 'alias',
          matchedAlias: raw,
        };
      }
    }

    // Fuzzy: alias contained in text (longest alias wins)
    /** @type {{ cap: object, alias: string, score: number }|null} */
    let best = null;
    for (const cap of this.list()) {
      if (enabledOnly && cap.enabled === false) continue;
      const aliases = [
        ...(Array.isArray(cap.missionAliases) ? cap.missionAliases : []),
        cap.name,
        String(cap.id).replace(/_/g, ' '),
      ].filter(Boolean);
      for (const alias of aliases) {
        const a = String(alias).toLowerCase().replace(/\s+/g, ' ').trim();
        if (a.length < 4) continue;
        if (lower.includes(a) || a.includes(lower)) {
          const score = a.length / Math.max(lower.length, a.length);
          if (!best || score > best.score || a.length > best.alias.length) {
            best = { cap, alias: String(alias), score };
          }
        }
      }
    }
    if (best) {
      return {
        known: true,
        capabilityId: best.cap.id,
        capability: best.cap,
        confidence: Math.min(0.94, 0.7 + best.score * 0.25),
        matchKind: 'fuzzy',
        matchedAlias: best.alias,
      };
    }

    return emptyAliasMatch();
  }

  /**
   * Suggest registered capabilities for unknown mission text (SPEC-054).
   * @param {string} text
   * @param {object} [opts]
   * @param {number} [opts.limit=5]
   * @param {boolean} [opts.enabledOnly=true]
   * @returns {{ id: string, name: string, score: number, matchedAlias: string|null }[]}
   */
  suggestMatches(text, opts = {}) {
    const raw = String(text || '').trim().toLowerCase();
    const limit = Number.isFinite(Number(opts.limit)) ? Number(opts.limit) : 5;
    const enabledOnly = opts.enabledOnly !== false;
    if (!raw) return [];

    const tokens = raw
      .split(/[^a-z0-9]+/)
      .map((t) => t.trim())
      .filter((t) => t.length >= 3);

    /** @type {Map<string, { id: string, name: string, score: number, matchedAlias: string|null }>} */
    const scored = new Map();
    for (const cap of this.list()) {
      if (enabledOnly && cap.enabled === false) continue;
      const aliases = [
        cap.id,
        cap.name,
        ...(Array.isArray(cap.missionAliases) ? cap.missionAliases : []),
      ]
        .filter(Boolean)
        .map((s) => String(s));
      let bestScore = 0;
      let matchedAlias = null;
      for (const alias of aliases) {
        const a = alias.toLowerCase();
        let score = 0;
        if (raw.includes(a) || a.includes(raw)) {
          score = 0.9;
        } else {
          const aliasTokens = a
            .split(/[^a-z0-9]+/)
            .filter((t) => t.length >= 3);
          const overlap = tokens.filter((t) =>
            aliasTokens.some((at) => at.includes(t) || t.includes(at))
          ).length;
          if (overlap) score = Math.min(0.85, 0.35 + overlap * 0.15);
        }
        if (score > bestScore) {
          bestScore = score;
          matchedAlias = alias;
        }
      }
      if (bestScore === 0) {
        if (/\bcampaign\b/.test(raw) && /campaign/i.test(cap.id + cap.name)) {
          bestScore = 0.4;
          matchedAlias = cap.name;
        } else if (/\bmail\b/.test(raw) && /mail/i.test(cap.id + cap.name)) {
          bestScore = 0.4;
          matchedAlias = cap.name;
        } else if (
          /\breview\b/.test(raw) &&
          /review/i.test(cap.id + cap.name)
        ) {
          bestScore = 0.35;
          matchedAlias = cap.name;
        }
      }
      if (bestScore > 0) {
        scored.set(cap.id, {
          id: cap.id,
          name: cap.name,
          score: bestScore,
          matchedAlias,
        });
      }
    }

    const ranked = [...scored.values()]
      .sort((a, b) => b.score - a.score || a.name.localeCompare(b.name))
      .slice(0, limit);

    if (ranked.length) return ranked;

    // SPEC-054: always offer operator-facing defaults for unknown phrases
    const defaults = [
      'campaign_builder',
      'mail_package_generator',
      'campaign_review',
      'business_intelligence',
      'prospect_discovery',
    ];
    const fallback = [];
    for (const id of defaults) {
      const cap = this.get(id);
      if (!cap) continue;
      if (enabledOnly && cap.enabled === false) continue;
      fallback.push({
        id: cap.id,
        name: cap.name,
        score: 0.2,
        matchedAlias: null,
      });
      if (fallback.length >= limit) break;
    }
    return fallback;
  }

  /**
   * Explain why a capability was not selected / lost ranking (SPEC-054).
   * @param {string} capabilityId
   * @param {object} [context]
   * @param {string} [context.artifactType]
   * @param {string} [context.selectedId]
   * @returns {object}
   */
  explainSelection(capabilityId, context = {}) {
    const id = String(capabilityId || '');
    const cap = this.get(id);
    const artifactType = context.artifactType || null;
    const selectedId = context.selectedId || null;

    if (!cap) {
      return {
        capabilityId: id,
        selected: false,
        reason: 'Capability not registered',
        causes: ['Capability not registered'],
        recommendedAction: `Register capability "${id}" in the Capability Registry.`,
      };
    }
    if (cap.enabled === false) {
      return {
        capabilityId: id,
        name: cap.name,
        selected: false,
        reason: 'Capability disabled',
        causes: ['Capability disabled'],
        recommendedAction: `Enable capability "${cap.name}" (${id}).`,
      };
    }
    if (artifactType) {
      const producers = this.producersOf(artifactType, { enabledOnly: false });
      const enabledProducers = producers.filter((p) => p.enabled !== false);
      const inProducers = producers.some((p) => p.id === id);
      if (!inProducers) {
        return {
          capabilityId: id,
          name: cap.name,
          selected: false,
          reason: 'Artifact contract mismatch',
          causes: [
            'Artifact contract mismatch',
            `Capability does not produce ${artifactType}`,
          ],
          recommendedAction: `Update "${cap.name}" produces contract to include ${artifactType}, or select a registered producer.`,
        };
      }
      if (selectedId && selectedId !== id) {
        const winner = this.get(selectedId);
        const winnerCost =
          (winner && Number(winner.acquisitionCost)) ||
          DEFAULT_ACQUISITION_COST[selectedId] ||
          500;
        const selfCost =
          Number(cap.acquisitionCost) ||
          DEFAULT_ACQUISITION_COST[id] ||
          500;
        return {
          capabilityId: id,
          name: cap.name,
          selected: false,
          reason: 'Lost compatibility ranking',
          causes: [
            `Selected ${
              (winner && winner.name) || selectedId
            } (cost ${winnerCost}) over ${cap.name} (cost ${selfCost})`,
          ],
          ranking: {
            selectedId,
            selectedName: (winner && winner.name) || selectedId,
            selectedCost: winnerCost,
            candidateCost: selfCost,
            registeredProducers: enabledProducers.map((p) => ({
              id: p.id,
              name: p.name,
              cost: p.acquisitionCost,
            })),
          },
          recommendedAction: `Prefer ${cap.name} by lowering its acquisition cost or disabling ${
            (winner && winner.name) || selectedId
          } if inappropriate.`,
        };
      }
    }
    return {
      capabilityId: id,
      name: cap.name,
      selected: true,
      reason: 'Eligible',
      causes: [],
      recommendedAction: null,
    };
  }

  /**
   * Resolve ordered capability ids; throws if any missing.
   * @param {string[]} ids
   * @returns {object[]}
   */
  resolveAll(ids) {
    return (ids || []).map((id) => {
      const cap = this.get(id);
      if (!cap) {
        throw new Error(formatMissingCapabilityError(id, this));
      }
      return cap;
    });
  }

  /**
   * Lightweight schema presence check (v1: required keys on inputs).
   * @param {string} capabilityId
   * @param {object} inputs
   * @returns {{ ok: boolean, missing: string[] }}
   */
  validateInputs(capabilityId, inputs) {
    const cap = this.get(capabilityId);
    if (!cap) return { ok: false, missing: ['capability'] };
    const required =
      (cap.inputSchema && Array.isArray(cap.inputSchema.required)
        ? cap.inputSchema.required
        : []) || [];
    const missing = required.filter(
      (key) => inputs == null || inputs[key] === undefined || inputs[key] === null
    );
    return { ok: missing.length === 0, missing };
  }
}

const DEFAULT_ACQUISITION_COST = Object.freeze({
  prospect_discovery: 100,
  company_enrichment: 80,
  knowledge_update: 40,
  opportunity_ranking: 70,
  business_intelligence: 55,
  sales_intelligence: 60,
  campaign_builder: 90,
  mail_package_generator: 50,
  campaign_review: 30,
  direct_mail_execution: 110,
  outcome_intelligence: 40,
  operator_inbox: 10,
  proposal_generator: 50,
});

/**
 * @param {object} capability
 */
function assertCapability(capability) {
  if (!capability || typeof capability !== 'object') {
    throw new Error('Capability must be an object');
  }
  for (const key of ['id', 'name', 'description', 'category']) {
    if (!capability[key]) throw new Error(`Capability missing ${key}`);
  }
  if (typeof capability.canRun !== 'function') {
    throw new Error(`Capability ${capability.id} missing canRun`);
  }
  if (typeof capability.estimate !== 'function') {
    throw new Error(`Capability ${capability.id} missing estimate`);
  }
  if (typeof capability.execute !== 'function') {
    throw new Error(`Capability ${capability.id} missing execute`);
  }
  if (capability.requires != null && !Array.isArray(capability.requires)) {
    throw new Error(`Capability ${capability.id} requires must be an array`);
  }
  if (capability.produces != null && !Array.isArray(capability.produces)) {
    throw new Error(`Capability ${capability.id} produces must be an array`);
  }
  if (
    capability.missionAliases != null &&
    !Array.isArray(capability.missionAliases)
  ) {
    throw new Error(
      `Capability ${capability.id} missionAliases must be an array`
    );
  }
  if (capability.enabled != null && typeof capability.enabled !== 'boolean') {
    throw new Error(`Capability ${capability.id} enabled must be a boolean`);
  }
  if (
    capability.version != null &&
    !Number.isFinite(Number(capability.version))
  ) {
    throw new Error(`Capability ${capability.id} version must be a number`);
  }
}

function normalizeTags(query) {
  if (Array.isArray(query)) return query.map(String);
  if (query && typeof query === 'object' && Array.isArray(query.outcomeTags)) {
    return query.outcomeTags.map(String);
  }
  if (typeof query === 'string') return [query];
  return [];
}

function normalizeArtifactKey(value) {
  return String(value || '')
    .trim()
    .replace(/([a-z])([A-Z])/g, '$1_$2')
    .replace(/[\s-]+/g, '_')
    .toLowerCase();
}

function artifactNeedles(artifactType) {
  const key = normalizeArtifactKey(artifactType);
  if (!key) return [];
  const needles = new Set([key]);
  if (key.endsWith('s')) needles.add(key.slice(0, -1));
  else needles.add(`${key}s`);
  return [...needles];
}

function emptyAliasMatch() {
  return {
    known: false,
    capabilityId: null,
    capability: null,
    confidence: 0,
    matchKind: null,
    matchedAlias: null,
  };
}

/**
 * Operator-facing error when a capability id is not in the registry.
 * @param {string} id
 * @param {CapabilityRegistry} registry
 */
function formatMissingCapabilityError(id, registry) {
  const suggestions = registry
    ? registry.suggestMatches(String(id || ''), { limit: 3 })
    : [];
  const lines = [
    `Capability not registered: ${id}`,
    'Status: Blocked',
    'Possible Causes: Capability not registered',
    `Recommended Action: Register capability "${id}" in the Capability Registry.`,
  ];
  if (suggestions.length) {
    lines.push(
      `Suggested Matches: ${suggestions.map((s) => s.name).join(', ')}`
    );
  }
  return lines.join(' ');
}

/**
 * @returns {CapabilityRegistry}
 */
function createCapabilityRegistry() {
  return new CapabilityRegistry();
}

module.exports = {
  CapabilityRegistry,
  createCapabilityRegistry,
  assertCapability,
  formatMissingCapabilityError,
  normalizeArtifactKey,
  DEFAULT_ACQUISITION_COST,
};
